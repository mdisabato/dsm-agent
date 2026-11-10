#!/usr/bin/env python3

import json
import time
import signal
import logging
import os
import subprocess
import ssl
import sys
import threading

import yaml
import paho.mqtt.client as mqtt

from collectors import dsm_disk_metrics
from collectors import dsm_service_metrics
from collectors import dsm_sensors
from collectors import dsm_ups
from controllers.dsm_commands import COMMAND_REGISTRY
from controllers import dsm_power

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class Agent:
    def __init__(self, config):
        self.config = config
        self.device = config["device"]["name"]
        self.interval = config.get("interval", 60)

        # runtime state
        self.running = False
        self.start_time = time.time()
        self.ups_monitor = None

        # Control-plane handlers: DSM commands + PoE power commands
        self.commands = {**COMMAND_REGISTRY, **dsm_power.make_commands(config)}

        # Home Assistant discovery
        self.discovery_prefix = config.get("homeassistant", {}).get(
            "discovery_prefix", "homeassistant"
        )

        # Device info per collector group
        self.sensor_device_info = {
            "identifiers": [f"{self.device}_sensor"],
            "name": f"{self.device} Sensors",
            "model": config["device"].get("model", "Synology NAS"),
            "manufacturer": config["device"].get("manufacturer", "Synology"),
        }

        self.service_device_info = {
            "identifiers": [f"{self.device}_service"],
            "name": f"{self.device} Services",
            "model": config["device"].get("model", "Synology NAS"),
            "manufacturer": config["device"].get("manufacturer", "dsm-agent"),
        }

        # Keep a default for backward compatibility
        self.device_info = self.sensor_device_info

        # MQTT topics — monitoring
        base_topic = config["mqtt"].get("base_topic", "system-sensors/sensor")
        self.state_topic = f"{base_topic}/{self.device}/state"
        self.status_topic = f"{base_topic}/{self.device}/status"

        service_base = config["mqtt"].get("service_base_topic", "system-services/sensor")
        self.service_state_topic = f"{service_base}/{self.device}/state"

        # MQTT topics — control plane
        self.command_topic = f"upd_control/{self.device}/command"
        self.command_status_topic = f"upd_control/{self.device}/status"

        # MQTT setup
        self.mqtt = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt.username_pw_set(
            config["mqtt"]["username"],
            config["mqtt"]["password"],
        )

        # TLS setup if configured
        tls_config = config.get("mqtt", {}).get("tls", {})
        if tls_config:
            ca_cert_path = tls_config.get("ca_cert")
            if not ca_cert_path:
                raise SystemExit(
                    "config.yaml has mqtt.tls set but tls.ca_cert is empty. "
                    "It needs to be the path this container sees — /app/ca.crt "
                    "if you're using the stock run.sh — not a path on the NAS."
                )
            if not os.path.isfile(ca_cert_path):
                raise SystemExit(
                    f"config.yaml's mqtt.tls.ca_cert is set to '{ca_cert_path}', "
                    "but that path doesn't exist inside this container. This is "
                    "almost always ca_cert pointing at a path on the Synology's "
                    "own filesystem (e.g. /volume1/docker/...) instead of the "
                    "container-side path it's bind-mounted to. run.sh mounts "
                    "your real ca.crt to /app/ca.crt inside the container — "
                    "config.yaml's ca_cert must say /app/ca.crt, regardless of "
                    "where the actual file lives on the NAS."
                )
            self.mqtt.tls_set(
                ca_certs=ca_cert_path,
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )

        # Last Will and Testament
        self.mqtt.will_set(
            self.status_topic,
            json.dumps({"action": "unexshutdown"}),
            qos=1,
            retain=True,
        )

        self.mqtt.on_connect = self._on_connect
        self.mqtt.on_message = self._on_message

    # ---------- Utility ----------

    def json(self, payload):
        return json.dumps(payload, separators=(",", ":"))

    # ---------- MQTT callbacks ----------

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        if reason_code != 0:
            logging.error(f"MQTT connection failed: {reason_code}")
            return

        logging.info("MQTT connected")

        # Subscribe to control plane commands
        self.mqtt.subscribe(self.command_topic)
        logging.info(f"Subscribed to {self.command_topic}")

        # Publish Home Assistant discovery
        dsm_disk_metrics.publish_discovery(self)
        logging.info("disk discovery published")

        dsm_sensors.publish_discovery(self)
        logging.info("sensors discovery published")

        dsm_service_metrics.publish_discovery(self)
        logging.info("service discovery published")

        dsm_ups.publish_discovery(self)
        logging.info("ups discovery published")

        self._publish_status_discovery()
        logging.info("status discovery published")

        self._publish_command_status_discovery()
        logging.info("command status discovery published")

        # Publish ready state
        self._publish_status("ready")
        logging.info("status: ready")

        from datetime import datetime
        self.publish_command_status({
            "host":            self.device,
            "command":         "ready",
            "status":          "ready",
            "step":            "online",
            "message":         "Agent online",
            "notification_id": "",
            "trace":           False,
            "timestamp":       datetime.now().isoformat(),
        })
        logging.info("command status: online")

    # ---------- Agent Status ----------

    def _publish_status_discovery(self) -> None:
        """Publish HA discovery for agent status sensor."""
        topic = (
            f"{self.discovery_prefix}/sensor/"
            f"{self.device}/agent_status/config"
        )

        payload = {
            "name": "Agent Status",
            "state_topic": self.status_topic,
            "value_template": "{{ value_json.action }}",
            "unique_id": f"{self.device}_agent_status",
            "device": self.service_device_info,
            "icon": "mdi:nas",
        }

        self.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)

    def _on_message(self, client, userdata, msg):
        """Receive command message and dispatch to registry handler in a thread."""
        try:
            payload = json.loads(msg.payload.decode())
            action = payload.get("action", "").lower()
            logging.info(f"Command received: {action} from {payload.get('host', 'unknown')}")

            handler = self.commands.get(action)
            if handler:
                thread = threading.Thread(
                    target=handler,
                    args=(payload, self.publish_command_status),
                    daemon=True,
                )
                thread.start()
            else:
                logging.warning(f"Unknown command: {action}")
                self.publish_command_status({
                    "host":            payload.get("host", self.device),
                    "command":         action,
                    "status":          "error",
                    "step":            "complete",
                    "message":         f"Unknown command: {action}",
                    "notification_id": payload.get("notification_id", ""),
                    "trace":           payload.get("trace", False),
                })
        except json.JSONDecodeError:
            logging.error(f"Invalid JSON in command message: {msg.payload.decode()}")
        except Exception as e:
            logging.error(f"Command handling failed: {e}")

    def publish_command_status(self, status_payload: dict) -> None:
        """Publish command status back to the control plane topic."""
        try:
            self.mqtt.publish(
                self.command_status_topic,
                json.dumps(status_payload),
                qos=1,
                retain=True,
            )
            logging.info(
                f"Command status published: {status_payload.get('command')} "
                f"[{status_payload.get('status')}] {status_payload.get('step')}"
            )
        except Exception as e:
            logging.error(f"Failed to publish command status: {e}")

    def _publish_command_status_discovery(self) -> None:
        """Publish HA discovery for four control plane sensors."""
        sensors = {
            "command_cmd": {
                "name": "Command",
                "value_template": "{{ value_json.command }}",
                "icon": "mdi:console",
            },
            "command_status": {
                "name": "Command Status",
                "value_template": "{{ value_json.status }}",
                "icon": "mdi:check-circle-outline",
            },
            "command_step": {
                "name": "Command Step",
                "value_template": "{{ value_json.step }}",
                "icon": "mdi:stairs",
            },
            "command_message": {
                "name": "Command Message",
                "value_template": "{{ value_json.message }}",
                "icon": "mdi:message-text-outline",
            },
        }

        for unique_suffix, sensor in sensors.items():
            topic = (
                f"{self.discovery_prefix}/sensor/"
                f"{self.device}/{unique_suffix}/config"
            )
            payload = {
                "name":           sensor["name"],
                "state_topic":    self.command_status_topic,
                "value_template": sensor["value_template"],
                "unique_id":      f"{self.device}_{unique_suffix}",
                "device":         self.service_device_info,
                "icon":           sensor["icon"],
            }
            self.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)

    def _publish_status(self, state: str) -> None:
        """Publish agent lifecycle status message."""
        payload = json.dumps({"action": state})
        self.mqtt.publish(self.status_topic, payload, qos=1, retain=True)

    def _check_system_state(self) -> str:
        """Detect if system is rebooting or shutting down."""
        try:
            result = subprocess.run(
                ["systemctl", "list-jobs"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            output = result.stdout.lower()

            if "reboot.target" in output:
                return "reboot"
            if "shutdown.target" in output or "poweroff.target" in output:
                return "shutdown"

            if os.path.exists("/run/systemd/reboot-mode"):
                return "reboot"

            result = subprocess.run(
                ["systemctl", "is-system-running"],
                capture_output=True,
                text=True,
                timeout=2,
            )
            if "stopping" in result.stdout.lower():
                return "shutdown"

        except (subprocess.TimeoutExpired, subprocess.SubprocessError, OSError) as e:
            logging.warning(f"Error checking system state: {e}")

        return "shutdown"

    # ---------- Signal handling ----------

    def _handle_signal(self, signum, frame):
        logging.info("Shutdown signal received")
        self.running = False

    # ---------- Main loop ----------

    def run(self):
        # Install signal handlers (only valid in main thread)
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

        # Connect to MQTT broker
        try:
            self.mqtt.connect(
                self.config["mqtt"]["broker"],
                self.config["mqtt"].get("port", 1883),
                60,
            )
        except Exception as e:
            logging.error(f"MQTT connect failed: {e}")
            sys.exit(1)  # non-zero so systemd Restart=on-failure triggers

        self.mqtt.loop_start()
        self.running = True

        # UPS watcher runs on its own fast loop (power events can't wait 60 s)
        if (self.config.get("ups") or {}).get("enabled", False):
            self.ups_monitor = dsm_ups.UpsMonitor(self)
            self.ups_monitor.start()

        while self.running:
            try:
                cycle_start = time.time()

                payload = {}
                payload.update(dsm_disk_metrics.collect(self))
                payload.update(dsm_sensors.collect(self))
                payload["agent_uptime"] = round(time.time() - self.start_time)
                payload["agent_cycle_ms"] = round((time.time() - cycle_start) * 1000, 1)

                self.mqtt.publish(
                    self.state_topic,
                    self.json(payload),
                    retain=True,
                )

                # Service metrics on separate topic
                service_payload = dsm_service_metrics.collect(self)
                self.mqtt.publish(
                    self.service_state_topic,
                    self.json(service_payload),
                    retain=True,
                )

                logging.info(f"state published ({len(payload)} metrics)")
            except Exception as e:
                logging.error(f"Collection cycle failed: {e}")

            # Interruptible sleep — exits within 1 second of SIGTERM
            for _ in range(self.interval):
                if not self.running:
                    break
                time.sleep(1)

        # Graceful shutdown
        if self.ups_monitor:
            self.ups_monitor.stop()
            self.ups_monitor.join(timeout=5)

        state = self._check_system_state()
        logging.info(f"Publishing shutdown state: {state}")
        self._publish_status(state)

        from datetime import datetime
        self.publish_command_status({
            "host":            self.device,
            "command":         state,
            "status":          "offline",
            "step":            "offline",
            "message":         f"Agent going offline — {state}",
            "notification_id": "",
            "trace":           False,
            "timestamp":       datetime.now().isoformat(),
        })
        logging.info("command status: offline")
        time.sleep(0.5)  # give MQTT time to deliver

        self.mqtt.loop_stop()
        time.sleep(0.5)  # final drain before disconnect
        self.mqtt.disconnect()
        logging.info("Agent stopped")


# ---------- Entry ----------


def load_config(path=None):
    if path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(script_dir, "config.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


if __name__ == "__main__":
    config = load_config()
    agent = Agent(config)
    agent.run()