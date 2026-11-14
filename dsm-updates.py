#!/usr/bin/env python3
"""
DSM Update Checker (one-shot)

Sensors (MQTT + HA discovery):
  - Software Updates  (count of updates, with attributes)
  - Package Updates   (same count, separate sensor)
  - Last Updated      (timestamp of last system update)
  - Last Checked      (timestamp when script last ran)
"""

import json
import logging
import os
import socket
import ssl
import subprocess
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt
import requests
import yaml
import pytz

# ---------- Constants ----------

TIMEZONE = pytz.timezone("America/Chicago")
HOSTNAME = socket.gethostname()
PKG_HISTORY_LOG = "/var/log/synopkg.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ---------- Config ----------


def load_config(path: Optional[str] = None) -> Dict[str, Any]:
    """
    Matches your config.yaml:

    device:
      name: homestone

    mqtt:
      broker: 10.1.10.183
      port: 8883
      tls:
        ca_cert: ...
      username: ...
      password: ...
      topic_prefix: system-updates

    homeassistant:
      discovery_prefix: homeassistant

    dsm:
      dsm_ip: ...
      dsm_port: ...
      dsm_username: ...
      dsm_password: ...
    """
    if path is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(script_dir, "config.yaml")
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------- MQTT ----------


def connect_mqtt(config: Dict[str, Any]) -> mqtt.Client:
    """Connect to MQTT broker and return connected client."""
    mqtt_cfg = config["mqtt"]
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(mqtt_cfg["username"], mqtt_cfg["password"])

    tls_cfg = mqtt_cfg.get("tls", {})
    if tls_cfg:
        client.tls_set(
            ca_certs=tls_cfg["ca_cert"],
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )

    connected = {"flag": False}

    def on_connect(c, userdata, flags, reason_code, properties=None):
        connected["flag"] = reason_code == 0
        if not connected["flag"]:
            logging.error(f"MQTT connection failed: {reason_code}")

    client.on_connect = on_connect
    client.connect(mqtt_cfg["broker"], mqtt_cfg.get("port", 1883), 60)
    client.loop_start()

    timeout = time.time() + 10
    while not connected["flag"] and time.time() < timeout:
        time.sleep(0.1)

    if not connected["flag"]:
        raise TimeoutError("Failed to connect to MQTT broker")

    logging.info("MQTT connected")
    return client


def publish(client: mqtt.Client, topic: str, payload: Any, retain: bool = True) -> None:
    """Publish JSON (or raw string) payload to MQTT."""
    if not isinstance(payload, str):
        payload = json.dumps(payload)
    result = client.publish(topic, payload, qos=1, retain=retain)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        logging.error(f"Publish failed on {topic}: {mqtt.error_string(result.rc)}")


# ---------- Discovery (4 sensors) ----------


def publish_discovery(client: mqtt.Client, config: Dict[str, Any]) -> None:
    """
    Publish Home Assistant discovery for:

    - software_updates
    - package_updates
    - last_updated
    - last_checked
    """
    ha_cfg = config.get("homeassistant", {})
    discovery_prefix = ha_cfg.get("discovery_prefix", ha_cfg.get("discovery_topic", "homeassistant"))

    mqtt_topic_prefix = config["mqtt"]["topic_prefix"]
    device_name = config.get("device", {}).get("name", HOSTNAME)

    sensors: Dict[str, Dict[str, Any]] = {
        "software_updates": {
            "name": "Software Updates",
            "icon": "mdi:update",
            "unique_id": f"{HOSTNAME}_software_updates",
            "state_topic": f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/software_updates/state",
            "value_template": "{{ value_json.software_updates }}",
            "json_attributes_topic": f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/software_updates/attributes",
            "json_attributes_template": "{{ value_json | tojson }}",
        },
        "package_updates": {
            "name": "Package Updates",
            "icon": "mdi:package-variant",
            "unique_id": f"{HOSTNAME}_package_updates",
            "state_topic": f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/package_updates/state",
            "value_template": "{{ value_json.package_updates }}",
        },
        "last_updated": {
            "name": "Last Updated",
            "icon": "mdi:clipboard-text-clock-outline",
            "unique_id": f"{HOSTNAME}_last_updated",
            "device_class": "timestamp",
            "state_topic": f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/last_updated/state",
            "value_template": "{{ value_json.last_updated }}",
        },
        "last_checked": {
            "name": "Last Checked",
            "icon": "mdi:clipboard-text-clock-outline",
            "unique_id": f"{HOSTNAME}_last_checked",
            "device_class": "timestamp",
            "state_topic": f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/last_checked/state",
            "value_template": "{{ value_json.last_checked }}",
        },
    }

    device_info = {
        "identifiers": [device_name],
        "name": f"{device_name} Software",
        "model": "Synology DSM",
        "manufacturer": "Synology",
    }

    for sensor, cfg in sensors.items():
        payload: Dict[str, Any] = {
            "name": str(cfg.get("name", "")),
            "state_topic": str(cfg.get("state_topic", "")),
            "value_template": str(cfg.get("value_template", "")),
            "icon": str(cfg.get("icon", "")),
            "unique_id": str(cfg.get("unique_id", "")),
            "device": device_info,
        }
        if "device_class" in cfg:
            payload["device_class"] = str(cfg["device_class"])
        if sensor == "software_updates":
            payload["json_attributes_topic"] = str(cfg.get("json_attributes_topic", ""))
            payload["json_attributes_template"] = str(cfg.get("json_attributes_template", ""))

        topic = f"{discovery_prefix}/sensor/{HOSTNAME}/{sensor}/config"
        publish(client, topic, payload)

    logging.info("Discovery published (Software, Package, Last Updated, Last Checked)")


# ---------- DSM API / synopkg ----------


def _dsm_url(config: Dict[str, Any], path: str) -> str:
    dsm = config["dsm"]
    return f"http://{dsm['dsm_ip']}:{dsm['dsm_port']}/webapi/{path}"


def login(config: Dict[str, Any]) -> Optional[str]:
    """Login to DSM and return session ID."""
    dsm = config["dsm"]
    try:
        response = requests.get(
            _dsm_url(config, "auth.cgi"),
            params={
                "api": "SYNO.API.Auth",
                "version": "3",
                "method": "login",
                "account": dsm["dsm_username"],
                "passwd": dsm["dsm_password"],
                "session": "PackageStation",
                "format": "sid",
            },
            timeout=30,
        )
        response.raise_for_status()
        sid = response.json().get("data", {}).get("sid")
        if sid:
            logging.info("DSM login successful")
        else:
            logging.error("DSM login returned no session ID")
        return sid
    except Exception as e:
        logging.error(f"DSM login failed: {e}")
        return None


def logout(config: Dict[str, Any], sid: str) -> None:
    if not sid:
        return
    try:
        requests.get(
            _dsm_url(config, "auth.cgi"),
            params={
                "api": "SYNO.API.Auth",
                "version": "1",
                "method": "logout",
                "session": "PackageStation",
                "_sid": sid,
            },
            timeout=10,
        )
    except Exception as e:
        logging.warning(f"DSM logout failed: {e}")


def run_sudo_command(command: List[str], password: str) -> subprocess.CompletedProcess:
    """Run a sudo command with password."""
    if not password:
        raise ValueError("Password is required for sudo commands")

    full_command = ["sudo", "-S"] + command
    logging.debug("Executing sudo command: %s", " ".join(full_command))
    result = subprocess.run(
        full_command,
        input=password + "\n",
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result


def check_package_updates(config: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
    """Run synopkg checkupdateall and return the list of available updates.

    Returns [] when synopkg reports nothing to update, and None when the check
    itself failed, so a failure is never published as "up to date".
    """
    dsm = config["dsm"]
    try:
        result = run_sudo_command(
            ["/usr/syno/bin/synopkg", "checkupdateall"],
            dsm["dsm_password"],
        )
        if result.returncode != 0:
            raise Exception(
                f"Command failed with return code {result.returncode}: {result.stderr}"
            )

        output = result.stdout.strip()
        json_start = output.find("[")
        if json_start == -1:
            json_start = output.find("{")
        if json_start == -1:
            logging.info("No updates available (no JSON in output)")
            return []

        clean_output = output[json_start:]
        updates = json.loads(clean_output)
        return updates if isinstance(updates, list) else []
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse synopkg output: {e}")
        logging.debug("Raw output: %s", output)
        return None
    except Exception as e:
        logging.error(f"synopkg checkupdateall failed: {e}")
        return None


def fetch_package_detail(
    config: Dict[str, Any], sid: str, package_id: str
) -> Optional[Dict[str, Any]]:
    """Fetch installed version info for a specific package."""
    if not sid:
        return None
    try:
        response = requests.get(
            _dsm_url(config, "entry.cgi"),
            params={
                "api": "SYNO.Core.Package",
                "version": "1",
                "method": "get",
                "id": package_id,
                "_sid": sid,
            },
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        return data.get("data") if data.get("success") else None
    except Exception as e:
        logging.error(f"Failed to fetch package detail for {package_id}: {e}")
        return None


def get_updates(
    config: Dict[str, Any], sid: str
) -> Tuple[Optional[int], List[Dict[str, Any]], Optional[str]]:
    """Return (count, details, error) of available package updates.

    count is None (and error says why) when the check failed.
    """
    raw_updates = check_package_updates(config)
    if raw_updates is None:
        return None, [], "synopkg update check failed"
    if not raw_updates:
        logging.info("No updates available.")
        return 0, [], None

    details: List[Dict[str, Any]] = []
    for pkg in raw_updates:
        if not isinstance(pkg, dict) or "id" not in pkg:
            continue
        info = fetch_package_detail(config, sid, pkg["id"]) or {}
        details.append(
            {
                "package_name": pkg.get("name", "unknown"),
                "installed_version": info.get("version", "unknown"),
                "latest_version": pkg.get("version", "unknown"),
            }
        )

    return len(details), details, None


def get_last_updated_time(config: Dict[str, Any]) -> str:
    """Get the timestamp of the last system update (timezone aware)."""
    dsm = config["dsm"]
    try:
        result = run_sudo_command(["cat", PKG_HISTORY_LOG], dsm["dsm_password"])
        if result.returncode != 0:
            raise Exception(
                f"Command failed with return code {result.returncode}: {result.stderr}"
            )

        lines = result.stdout.splitlines()
        for line in reversed(lines):
            if "pgrade" in line:
                date_str = " ".join(line.split()[:2])
                dt = datetime.strptime(date_str, "%Y/%m/%d %H:%M:%S")
                return TIMEZONE.localize(dt).isoformat()
    except Exception as e:
        logging.error(f"Failed to get last updated time: {e}")

    return datetime.now(TIMEZONE).isoformat()


# ---------- Publish metrics (4 sensors) ----------


def publish_metrics(
    client: mqtt.Client,
    config: Dict[str, Any],
    state: Optional[int],
    attributes: List[Dict[str, Any]],
    error: Optional[str] = None,
) -> None:
    """
    Publish metrics to MQTT:

    - last_updated: timestamp
    - last_checked: timestamp
    - software_updates: state (count)
    - package_updates: state (count, separate sensor)

    A failed check (state None) publishes null counts, which Home Assistant
    shows as unknown, and the reason in the attributes.
    """
    mqtt_topic_prefix = config["mqtt"]["topic_prefix"]
    now = datetime.now(TIMEZONE)

    metrics = {
        "last_updated": get_last_updated_time(config),
        "last_checked": now.isoformat(),
        "software_updates": state,
        "package_updates": state,
    }

    # Per-metric state topics
    for metric, value in metrics.items():
        topic = f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/{metric}/state"
        payload = {metric: value}
        publish(client, topic, payload)
        logging.info("Published %s: %s", metric, value)

    # Attributes for software_updates
    attributes_topic = f"{mqtt_topic_prefix}/sensor/{HOSTNAME}/software_updates/attributes"
    if error:
        publish(client, attributes_topic, {"updates": [], "error": error})
        logging.warning("Update check failed, published unknown: %s", error)
    elif state:
        publish(client, attributes_topic, {"updates": attributes})
        logging.info(
            "Published software_updates attributes for %d updates", len(attributes)
        )
    else:
        publish(client, attributes_topic, {"updates": []})
        logging.info("Cleared software_updates attributes")


# ---------- Main ----------


def main() -> None:
    config = load_config()
    client: Optional[mqtt.Client] = None
    sid: Optional[str] = None

    try:
        client = connect_mqtt(config)
        publish_discovery(client, config)

        sid = login(config)
        if not sid:
            logging.error("Cannot check updates — DSM login failed")
            publish_metrics(client, config, None, [], "DSM login failed")
            sys.exit(1)

        state, attributes, error = get_updates(config, sid)
        publish_metrics(client, config, state, attributes, error)

    except Exception as e:
        logging.error("Fatal error: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        if sid:
            logout(config, sid)
        if client:
            time.sleep(0.5)
            client.loop_stop()
            client.disconnect()
        logging.info("dsm_updates one-shot complete")


if __name__ == "__main__":
    main()
