import docker
import yaml
import json
import time
import ssl
import paho.mqtt.client as mqtt
from typing import Dict, Any
import os
from datetime import datetime
import logging
import sys
import threading
import signal
from functools import wraps

def exponential_backoff(max_attempts: int = 5, base_delay: int = 1):
    """Decorator for exponential backoff retry logic."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = base_delay
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        wait_time = delay * (2 ** attempt)
                        logging.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s")
                        time.sleep(wait_time)
                    else:
                        logging.error(f"All {max_attempts} attempts failed")
                        raise last_exception
            return None
        return wrapper
    return decorator

class DockerMonitor:
    def __init__(self, config_path: str = '/app/config.yaml') -> None:
        """Initialize the Docker monitor from config.yaml."""
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        # Device identity
        self.device_name = self.config['device']['name']

        # MQTT settings
        mqtt_cfg = self.config['mqtt']
        self.mqtt_host = mqtt_cfg['broker']
        self.mqtt_port = mqtt_cfg.get('port', 1883)
        self.mqtt_user = mqtt_cfg.get('username')
        self.mqtt_pass = mqtt_cfg.get('password')
        self.mqtt_tls = mqtt_cfg.get('tls', {})
        self.connected = False

        # Home Assistant discovery prefix
        self.discovery_prefix = self.config.get('homeassistant', {}).get(
            'discovery_prefix', 'homeassistant'
        )

        # Collector interval for docker
        self.interval = (
            self.config.get('collectors', {})
                       .get('docker', {})
                       .get('interval_seconds', 60)
        )

        # Control flags
        self.running = True
        self.healthy = True

        # Docker client via local socket
        self.docker_client = docker.DockerClient(
            base_url='unix:///var/run/docker.sock',
            timeout=30
        )

        # Previous network stats for rate calculation
        self._prev_net_stats = {}  # {container_id: {'rx': bytes, 'tx': bytes, 'ts': epoch}}

        # MQTT client setup
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_reconnect_delay = 1
        self.max_reconnect_delay = 300
        self.should_reconnect = True

        # Setup signal handlers
        signal.signal(signal.SIGTERM, self.handle_shutdown)
        signal.signal(signal.SIGINT, self.handle_shutdown)

    def handle_shutdown(self, signum, frame) -> None:
        """Handle shutdown signals gracefully."""
        logging.info("Shutdown signal received, cleaning up...")
        self.running = False
        self.cleanup()
        sys.exit(0)

    def cleanup(self) -> None:
        """Cleanup resources."""
        logging.info("Performing cleanup...")
        self.running = False
        self.should_reconnect = False
        
        # Clean up MQTT
        if hasattr(self, 'mqtt_client'):
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
            except Exception as e:
                logging.error(f"Error during MQTT cleanup: {e}")
                
        # Clean up Docker client
        if hasattr(self, 'docker_client'):
            try:
                self.docker_client.close()
            except Exception as e:
                logging.error(f"Error during Docker client cleanup: {e}")

    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties) -> None:
        """Callback when MQTT connects."""
        self.connected = reason_code == 0
        if self.connected:
            self.mqtt_reconnect_delay = 1  # Reset delay on successful connection
            logging.info("Successfully connected to MQTT broker")
        else:
            logging.error(f"Failed to connect to MQTT broker with code {reason_code}")

    def on_mqtt_disconnect(self, client, userdata, flags, reason_code, properties) -> None:
        """Callback when MQTT disconnects."""
        self.connected = False
        if reason_code != 0 and self.should_reconnect:
            reconnect_delay = min(self.mqtt_reconnect_delay * 2, self.max_reconnect_delay)
            logging.warning(f"Unexpected MQTT disconnection (code: {reason_code}). Reconnecting in {reconnect_delay}s")
            time.sleep(reconnect_delay)
            self.mqtt_reconnect_delay = reconnect_delay
            try:
                self.mqtt_client.reconnect()
            except Exception as e:
                logging.error(f"Reconnection failed: {e}")
        else:
            logging.info("Clean MQTT disconnection")
            self.mqtt_reconnect_delay = 1

    def setup_mqtt(self, max_retries: int = 5, retry_delay: int = 5) -> None:
        """Setup MQTT connection with retry logic."""
        self.mqtt_client.on_connect = self.on_mqtt_connect
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
        
        if self.mqtt_user and self.mqtt_pass:
            self.mqtt_client.username_pw_set(self.mqtt_user, self.mqtt_pass)

        # TLS setup if configured
        if self.mqtt_tls:
            self.mqtt_client.tls_set(
                ca_certs=self.mqtt_tls["ca_cert"],
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )
            logging.info("TLS enabled")
        
        retry_count = 0
        while retry_count < max_retries and self.running:
            try:
                logging.info(f"Connecting to MQTT broker at {self.mqtt_host}:{self.mqtt_port}")
                self.mqtt_client.connect(self.mqtt_host, self.mqtt_port, 60)
                self.mqtt_client.loop_start()
                
                # Wait for connection
                connection_timeout = 10
                connection_start_time = time.time()
                while not self.connected and self.running:
                    if time.time() - connection_start_time > connection_timeout:
                        raise TimeoutError("Failed to connect within timeout period")
                    time.sleep(0.1)
                
                return
                
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries and self.running:
                    logging.error(f"MQTT connection failed: {str(e)}")
                    logging.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logging.critical("Failed to connect to MQTT after multiple attempts")
                    raise

    @exponential_backoff(max_attempts=3, base_delay=1)
    def publish_mqtt_message(self, topic: str, payload: Dict, retain: bool = True) -> bool:
        """Publish MQTT message with retry logic."""
        if not self.connected:
            logging.error("Cannot publish: Not connected to MQTT broker")
            return False

        try:
            result = self.mqtt_client.publish(
                topic, 
                json.dumps(payload), 
                qos=1, 
                retain=retain
            )
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                return True
            raise Exception(f"MQTT publish failed with code {result.rc}")
        except Exception as e:
            logging.error(f"Error publishing message: {str(e)}")
            raise

    @exponential_backoff(max_attempts=3, base_delay=1)
    def get_containers(self) -> list:
        """Get all containers from local Docker daemon."""
        try:
            containers = self.docker_client.containers.list(all=True)
            logging.debug(f"Retrieved {len(containers)} containers from {self.device_name}")
            return containers
        except docker.errors.DockerException as e:
            logging.error(f"Failed to get containers from {self.device_name}: {str(e)}")
            raise

    @exponential_backoff(max_attempts=3, base_delay=1)
    def get_container_stats(self, container_id: str) -> Dict[str, Any]:
        """Get stats for a specific container."""
        try:
            container = self.docker_client.containers.get(container_id)
            stats = container.stats(stream=False)
            
            cpu_stats = stats.get('cpu_stats', {})
            precpu_stats = stats.get('precpu_stats', {})
            memory_stats = stats.get('memory_stats', {})

            # Calculate CPU percent
            cpu_percent = 0.0
            try:
                cpu_delta = cpu_stats['cpu_usage']['total_usage'] - \
                           precpu_stats['cpu_usage']['total_usage']
                system_delta = cpu_stats['system_cpu_usage'] - \
                              precpu_stats['system_cpu_usage']
                
                # cgroups v2 uses online_cpus, v1 uses percpu_usage
                num_cpus = cpu_stats.get('online_cpus') or \
                          len(cpu_stats['cpu_usage'].get('percpu_usage', [1]))
                
                if system_delta > 0:
                    cpu_percent = (cpu_delta / system_delta) * 100.0 * num_cpus
                    cpu_percent = min(cpu_percent, 100.0 * num_cpus)
            except (KeyError, TypeError):
                logging.debug(f"CPU stats unavailable for container {container_id}")

            # Calculate memory percent - handle both cgroups v1 and v2
            mem_percent = 0.0
            mem_usage = memory_stats.get('usage', 0)
            mem_limit = memory_stats.get('limit', 0)

            # cgroups v2: calculate usage from anon + file
            if not mem_usage:
                inner_stats = memory_stats.get('stats', {})
                mem_usage = inner_stats.get('anon', 0) + inner_stats.get('file', 0)

            # No per-container limit: fall back to system total memory
            if not mem_limit:
                try:
                    mem_limit = os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES')
                except (ValueError, OSError):
                    mem_limit = 0

            if mem_usage and mem_limit:
                mem_percent = (mem_usage / mem_limit) * 100.0

            # Calculate network I/O rates (bits/sec)
            network_rx = 0
            network_tx = 0
            networks = stats.get('networks', {})
            for iface_stats in networks.values():
                network_rx += iface_stats.get('rx_bytes', 0)
                network_tx += iface_stats.get('tx_bytes', 0)

            now = time.time()
            prev = self._prev_net_stats.get(container_id)
            if prev:
                elapsed = now - prev['ts']
                if elapsed > 0:
                    rx_bps = max(0.0, ((network_rx - prev['rx']) / elapsed) * 8)
                    tx_bps = max(0.0, ((network_tx - prev['tx']) / elapsed) * 8)
                else:
                    rx_bps = 0.0
                    tx_bps = 0.0
            else:
                rx_bps = 0.0
                tx_bps = 0.0

            self._prev_net_stats[container_id] = {'rx': network_rx, 'tx': network_tx, 'ts': now}

            # Calculate block I/O
            block_read = 0
            block_write = 0
            blkio = stats.get('blkio_stats', {})
            for entry in blkio.get('io_service_bytes_recursive', []) or []:
                if entry.get('op') == 'read':
                    block_read += entry.get('value', 0)
                elif entry.get('op') == 'write':
                    block_write += entry.get('value', 0)

            return {
                'cpu_percent': round(cpu_percent, 2),
                'memory_percent': round(mem_percent, 2),
                'network_rx_bps': round(rx_bps, 1),
                'network_tx_bps': round(tx_bps, 1),
                'block_read_bytes': block_read,
                'block_write_bytes': block_write
            }
        except Exception as e:
            logging.error(f"Failed to get container stats: {str(e)}")
            raise

    def publish_discovery_config(self, endpoint_name: str, container_name: str) -> None:
        """Publish MQTT discovery configuration for Home Assistant."""
        logging.debug(f"Creating discovery config for {container_name} on {endpoint_name}")
        
        # Clean up names
        clean_container = container_name.replace('-', '_').replace('.', '_').lstrip('/')
        clean_endpoint = endpoint_name.replace('-', '_').replace('.', '_')
        
        # Create identifiers
        host_id = f"{clean_endpoint}_docker"
        entity_id = f"{clean_endpoint}_docker_{clean_container}"
        
        # Create device info
        device_info = {
            "identifiers": [host_id],
            "name": f"{endpoint_name} Docker",
            "manufacturer": "Docker",
            "model": "Host",
        }

        # Define sensor configuration
        discovery_topic = f"{self.discovery_prefix}/sensor/{entity_id}/config"
        
        payload = {
            "name": f"{container_name}",
            "unique_id": entity_id,
            "state_topic": f"docker/{endpoint_name}/{container_name}/state",
            "icon": "mdi:docker",
            "device": device_info,
            "json_attributes_topic": f"docker/{endpoint_name}/{container_name}/attributes",
            "value_template": "{{ value_json.state }}"
        }

        self.publish_mqtt_message(discovery_topic, payload, retain=True)

    def process_container(self, endpoint_name: str, container) -> None:
        """Process a single container and publish its data."""
        try:
            container_name = container.name
            
            # Publish discovery config for container
            self.publish_discovery_config(endpoint_name, container_name)
            
            state_data = {
                "state": container.status.capitalize()
            }

            attributes = {
                "status": container.attrs['State']['Status'],
                "last_updated": datetime.now().isoformat()
            }

            # Health status (only present if HEALTHCHECK defined)
            health = container.attrs['State'].get('Health', {})
            attributes['health_status'] = health.get('Status', 'none')

            # Restart count
            attributes['restart_count'] = container.attrs.get('RestartCount', 0)

            # Uptime (from container start time)
            started_at = container.attrs['State'].get('StartedAt', '')
            if started_at and container.status == 'running':
                try:
                    start_time = datetime.fromisoformat(
                        started_at.replace('Z', '+00:00')
                    )
                    delta = datetime.now(start_time.tzinfo) - start_time
                    uptime_secs = int(delta.total_seconds())
                    attributes['uptime_seconds'] = uptime_secs

                    # Human-readable uptime
                    days, remainder = divmod(uptime_secs, 86400)
                    hours, remainder = divmod(remainder, 3600)
                    minutes, _ = divmod(remainder, 60)
                    parts = []
                    if days:
                        parts.append(f"{days}d")
                    if hours:
                        parts.append(f"{hours}h")
                    parts.append(f"{minutes}m")
                    attributes['uptime_human'] = ' '.join(parts)
                except (ValueError, TypeError):
                    attributes['uptime_seconds'] = 0
                    attributes['uptime_human'] = 'unknown'
            else:
                attributes['uptime_seconds'] = 0
                attributes['uptime_human'] = 'stopped'

            # Get stats for running containers
            if container.status == 'running':
                try:
                    stats = self.get_container_stats(container.id)
                    attributes.update(stats)
                except Exception as e:
                    logging.error(f"Failed to get stats for container {container_name}: {e}")
                    attributes.update({
                        'cpu_percent': 0.0,
                        'memory_percent': 0.0,
                        'network_rx_bps': 0.0,
                        'network_tx_bps': 0.0,
                        'block_read_bytes': 0,
                        'block_write_bytes': 0,
                        'error': str(e)
                    })
            
            base_topic = f"docker/{endpoint_name}/{container_name}"
            
            self.publish_mqtt_message(f"{base_topic}/state", state_data)
            self.publish_mqtt_message(f"{base_topic}/attributes", attributes)
            
        except Exception as e:
            logging.error(f"Error processing container {container_name}: {str(e)}")
            self.healthy = False

    def monitor_loop(self) -> None:
        """Main monitoring loop."""
        logging.info(f"Starting monitoring loop with {self.interval} second interval")
        last_run_time = 0
        
        while self.running:
            try:
                current_time = time.time()
                
                if current_time - last_run_time < self.interval:
                    sleep_time = self.interval - (current_time - last_run_time)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
                
                loop_start_time = time.time()

                try:
                    containers = self.get_containers()
                    
                    for container in containers:
                        if not self.running:
                            break
                        
                        try:
                            self.process_container(self.device_name, container)
                        except Exception as e:
                            logging.error(f"Error processing container on {self.device_name}: {e}")
                            continue
                        
                except Exception as e:
                    logging.error(f"Error retrieving containers from {self.device_name}: {str(e)}")
                
                loop_duration = time.time() - loop_start_time
                logging.debug(f"Monitoring loop completed in {loop_duration:.2f} seconds")
                
                last_run_time = loop_start_time
                
            except Exception as e:
                logging.error(f"Error in monitoring loop: {str(e)}")
                self.healthy = False
                time.sleep(5)

def main():
    """Main function to run the Docker monitor."""
    # Configure logging
    log_level = logging.DEBUG if os.getenv('DEBUG', '').lower() == 'true' else logging.INFO
    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=log_level,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Configuration file path (can be overridden via environment variable)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.getenv('CONFIG_PATH', os.path.join(script_dir, 'config.yaml'))

    if not os.path.exists(config_path):
        logging.critical(f"Configuration file not found: {config_path}")
        sys.exit(1)

    monitor = None
    try:
        monitor = DockerMonitor(config_path=config_path)
        monitor.setup_mqtt()
        monitor.monitor_loop()
        
    except KeyboardInterrupt:
        logging.info("Shutting down...")
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        sys.exit(1)
    finally:
        if monitor:
            monitor.cleanup()
        sys.exit(0)

if __name__ == "__main__":
    main()