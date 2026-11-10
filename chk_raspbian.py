#!/usr/bin/env python3
"""
chk_raspbian.py — One-shot script. Fetches current Raspberry Pi OS release
info from the web and publishes to MQTT for HA discovery.

Sensors are shared across all Pis — runs on homestone on a weekly schedule.
"""

import json
import logging
import os
import ssl
import time
from datetime import datetime
from typing import Any, Dict

import paho.mqtt.client as mqtt
import requests
import yaml
from zoneinfo import ZoneInfo

# ---------- Constants ----------

TIMEZONE = ZoneInfo("America/Chicago")
CONFIG_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), "config.yaml")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


# ---------- Config ----------

def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


# ---------- MQTT ----------

def connect_mqtt(config: Dict[str, Any]) -> mqtt.Client:
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
            logger.error(f"MQTT connection failed: {reason_code}")

    client.on_connect = on_connect
    client.connect(mqtt_cfg["broker"], mqtt_cfg.get("port", 1883), 60)
    client.loop_start()

    timeout = time.time() + 10
    while not connected["flag"] and time.time() < timeout:
        time.sleep(0.1)

    if not connected["flag"]:
        raise TimeoutError("Failed to connect to MQTT broker")

    logger.info("MQTT connected")
    return client


def publish(client: mqtt.Client, topic: str, payload: Any, retain: bool = True) -> None:
    if not isinstance(payload, str):
        payload = json.dumps(payload)
    result = client.publish(topic, payload, qos=1, retain=retain)
    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        logger.error(f"Publish failed on {topic}: {mqtt.error_string(result.rc)}")


# ---------- Raspbian info ----------

def get_debian_codename() -> str:
    try:
        response = requests.get(
            "https://deb.debian.org/debian/dists/stable/Release", timeout=10
        )
        response.raise_for_status()
        for line in response.text.splitlines():
            if line.startswith("Codename:"):
                return line.split("Codename:")[1].strip()
    except Exception as e:
        logger.error(f"Error fetching Debian codename: {e}")
    return "Unknown"


def get_raspbian_info() -> Dict[str, str]:
    response = requests.get(
        "https://downloads.raspberrypi.com/raspios_armhf/release_notes.txt",
        timeout=10,
    )
    response.raise_for_status()
    lines = response.text.split("\n")

    raw_date = lines[0].strip().rstrip(":")
    release_date = f"{raw_date}T00:00:00+00:00"

    kernel_version = next(
        (
            line.split("kernel ")[1].strip().split(" - ")[0]
            for line in lines
            if "kernel " in line
        ),
        "Unknown",
    )

    return {
        "release_date":    release_date,
        "kernel_version":  kernel_version,
        "debian_codename": get_debian_codename(),
        "last_checked":    datetime.now(TIMEZONE).replace(microsecond=0).isoformat(),
    }


# ---------- Discovery ----------

def publish_discovery(client: mqtt.Client, config: Dict[str, Any]) -> None:
    discovery_prefix = config.get("homeassistant", {}).get("discovery_prefix", "homeassistant")
    base_topic = config["mqtt"].get("base_topic", "system-sensors/sensor")
    state_topic_base = f"{base_topic}/raspbian"

    device_info = {
        "identifiers": ["raspbian_os_monitor"],
        "name":         "Raspbian OS Monitor",
        "manufacturer": "Raspberry Pi Foundation",
        "model":        "OS Monitor",
    }

    sensors = [
        ("release_date",    "Release Date",   "mdi:calendar",      {"device_class": "timestamp", "entity_category": "diagnostic"}),
        ("kernel_version",  "OS Version",     "mdi:linux",         {"entity_category": "diagnostic"}),
        ("debian_codename", "Codename",       "mdi:debian",        {"entity_category": "diagnostic"}),
        ("last_checked",    "Last Checked",   "mdi:clock-outline", {"device_class": "timestamp", "entity_category": "diagnostic"}),
    ]

    for key, name, icon, extra in sensors:
        topic = f"{discovery_prefix}/sensor/raspbian/{key}/config"
        payload = {
            "name":           f"Raspbian {name}",
            "unique_id":      f"raspbian_os_{key}",
            "state_topic":    f"{state_topic_base}/{key}/state",
            "value_template": "{{ value }}",
            "icon":           icon,
            "device":         device_info,
            **extra,
        }
        publish(client, topic, payload)

    logger.info("Discovery published")


# ---------- Main ----------

def main() -> None:
    config = load_config()
    client = None

    try:
        client = connect_mqtt(config)
        info = get_raspbian_info()

        publish_discovery(client, config)

        base_topic = config["mqtt"].get("base_topic", "system-sensors/sensor")
        for key, value in info.items():
            topic = f"{base_topic}/raspbian/{key}/state"
            publish(client, topic, value)
            logger.info(f"Published {key}: {value}")

    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if client:
            time.sleep(0.5)
            client.loop_stop()
            client.disconnect()
        logger.info("chk_raspbian complete")


if __name__ == "__main__":
    main()