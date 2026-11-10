#!/usr/bin/env python3

import json
import logging
import asyncio
import aiohttp
import psutil

from synology_dsm import SynologyDSM


# ---------- Discovery ----------


def publish_discovery(agent):
    """Publish HA discovery for configured volume mount points.
    USB device discovery is handled dynamically in collect().
    """
    mounts = agent.config.get("mounts", {})
    if not mounts:
        logging.warning("No mounts defined in config.yaml")
        return

    for label, mountpoint in mounts.items():
        safe = _safe(label)
        key = f"disk_{safe}"
        _publish_sensor(agent, f"{key}_pct",   f"{label} Used %", f"{{{{ value_json.{key}_pct }}}}",   "harddisk",               unit="%", state_class="measurement")
        _publish_sensor(agent, f"{key}_free",  f"{label} Free",   f"{{{{ value_json.{key}_free }}}}",  "harddisk-plus-outline")
        _publish_sensor(agent, f"{key}_total", f"{label} Total",  f"{{{{ value_json.{key}_total }}}}", "harddisk-plus")


def _publish_sensor(agent, key, name, value_template, icon, unit=None, state_class=None):
    topic = f"{agent.discovery_prefix}/sensor/{agent.device}/{key}/config"
    payload = {
        "name":           name,
        "state_topic":    agent.state_topic,
        "value_template": value_template,
        "unique_id":      f"{agent.device}_{key}",
        "device":         agent.sensor_device_info,
        "icon":           f"mdi:{icon}",
    }
    if unit:
        payload["unit_of_measurement"] = unit
    if state_class:
        payload["state_class"] = state_class
    agent.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)


# ---------- Collection ----------


def collect(agent):
    """Collect volume metrics via psutil and USB metrics via synology_dsm API."""
    data = {}

    # --- Volume metrics ---
    # config.yaml's bare "mounts:" (no value) parses as None in YAML, not
    # {} — .get()'s default only applies when the key is absent entirely.
    # publish_discovery() already guards against this; collect() didn't.
    mounts = agent.config.get("mounts") or {}
    for label, mountpoint in mounts.items():
        safe = _safe(label)
        key = f"disk_{safe}"
        try:
            usage = psutil.disk_usage(mountpoint)
            data[f"{key}_pct"]   = round(usage.percent, 1)
            data[f"{key}_free"]  = _fmt(usage.free)
            data[f"{key}_total"] = _fmt(usage.total)
        except Exception as e:
            logging.warning(f"Disk metric failed for {mountpoint}: {e}")
            data[f"{key}_pct"]   = None
            data[f"{key}_free"]  = None
            data[f"{key}_total"] = None

    # --- USB metrics ---
    try:
        usb_devices = asyncio.run(_fetch_usb_devices(agent.config))
        _publish_usb(agent, usb_devices)
    except Exception as e:
        logging.error(f"USB device collection failed: {e}")

    return data


# ---------- USB fetch (async) ----------


async def _fetch_usb_devices(config):
    """Fetch USB device data from synology_dsm API."""
    dsm_cfg = config.get("dsm", {})
    devices = {}

    async with aiohttp.ClientSession() as session:
        api = SynologyDSM(
            session,
            dsm_cfg["dsm_ip"],
            dsm_cfg["dsm_port"],
            dsm_cfg["dsm_username"],
            dsm_cfg["dsm_password"],
            use_https=False,
            timeout=30,
        )
        await api.login()
        await api.external_usb.update()

        for device_id, device_data in api.external_usb.get_devices.items():
            try:
                data_dict = getattr(device_data, "_data", {})

                device = {
                    "device_id":    device_id,
                    "device_name":  device_data.device_name,
                    "manufacturer": data_dict.get("producer", "Unknown"),
                    "model":        data_dict.get("product", "Generic USB Storage"),
                    "is_mounted":   data_dict.get("status", "normal") == "normal",
                    "partitions":   {},
                }

                total_bytes = 0
                used_bytes  = 0

                for part_id, part_data in device_data.device_partitions.items():
                    part_size = part_data.partition_size_total()
                    part_used = part_data.partition_size_used()
                    part_free = part_size - part_used
                    total_bytes += part_size
                    used_bytes  += part_used

                    part_dict = getattr(part_data, "_data", {})
                    if part_dict.get("status", "normal") != "normal":
                        device["is_mounted"] = False

                    device["partitions"][part_id] = {
                        "share_name":       part_data.share_name,
                        "filesystem":       part_data.filesystem,
                        "size_total":       part_size,
                        "size_used":        part_used,
                        "size_free":        part_free,
                        "percentage_used":  part_data.partition_percentage_used,
                        "size_total_human": _fmt(part_size),
                        "size_used_human":  _fmt(part_used),
                        "size_free_human":  _fmt(part_free),
                    }

                # Use share name as device name when there is a single partition
                if len(device["partitions"]) == 1:
                    part = next(iter(device["partitions"].values()))
                    if part["share_name"]:
                        device["device_name"] = part["share_name"]

                free_bytes = total_bytes - used_bytes
                device["total_size"]       = total_bytes
                device["used_size"]        = used_bytes
                device["free_size"]        = free_bytes
                device["percentage_used"]  = round((used_bytes / total_bytes) * 100, 1) if total_bytes else 0.0
                device["total_size_human"] = _fmt(total_bytes)
                device["used_size_human"]  = _fmt(used_bytes)
                device["free_size_human"]  = _fmt(free_bytes)

                if device["partitions"]:
                    devices[device_id] = device

            except Exception as e:
                logging.error(f"Error processing USB device {device_id}: {e}")

    return devices


# ---------- USB publish ----------


def _publish_usb(agent, usb_devices):
    """Publish HA discovery and state for each USB device."""
    base_topic = agent.config["mqtt"].get("base_topic", "system-sensors/sensor")

    for device_id, device in usb_devices.items():
        sanitized   = device["device_name"].replace(" ", "_").lower()
        unique_id   = f"{agent.device}_{sanitized}"
        state_topic = f"{base_topic}/{unique_id}/state"

        device_info = {
            "identifiers": [unique_id],
            "name":         f"{agent.device} {device['device_name']}",
            "model":        device["model"],
            "manufacturer": device["manufacturer"],
            "via_device":   f"{agent.device}_sensor",
        }

        # Regular sensors
        for sensor_id, name, value_tmpl, icon, extra in [
            ("usage", "Usage", "{{ value_json.percentage_used }}",  "harddisk",               {"unit_of_measurement": "%", "state_class": "measurement"}),
            ("size",  "Size",  "{{ value_json.total_size_human }}", "harddisk-plus",           {}),
            ("used",  "Used",  "{{ value_json.used_size_human }}",  "harddisk-remove",         {}),
            ("free",  "Free",  "{{ value_json.free_size_human }}",  "harddisk-plus-outline",   {}),
        ]:
            disc_topic = f"{agent.discovery_prefix}/sensor/{unique_id}/{sensor_id}/config"
            payload = {
                "name":           name,
                "state_topic":    state_topic,
                "value_template": value_tmpl,
                "unique_id":      f"{unique_id}_{sensor_id}",
                "device":         device_info,
                "icon":           f"mdi:{icon}",
                **extra,
            }
            agent.mqtt.publish(disc_topic, json.dumps(payload), qos=1, retain=True)

        # Binary sensor — connected
        bin_topic = f"{agent.discovery_prefix}/binary_sensor/{unique_id}/connected/config"
        agent.mqtt.publish(bin_topic, json.dumps({
            "name":           "Connected",
            "state_topic":    state_topic,
            "value_template": "{{ value_json.is_mounted }}",
            "unique_id":      f"{unique_id}_connected",
            "device":         device_info,
            "device_class":   "connectivity",
            "payload_on":     True,
            "payload_off":    False,
            "icon":           "mdi:usb",
        }), qos=1, retain=True)

        # State payload
        agent.mqtt.publish(state_topic, json.dumps(device), qos=1, retain=True)
        logging.info(f"USB device published: {device['device_name']}")


# ---------- Helpers ----------


def _fmt(size_bytes: int) -> str:
    """Format bytes to human-readable string."""
    if size_bytes < 1:
        return "0 MB"
    tb = 1024 ** 4
    gb = 1024 ** 3
    mb = 1024 ** 2
    kb = 1024
    if size_bytes >= 900 * gb:
        return f"{size_bytes / tb:.2f} TB"
    elif size_bytes >= gb:
        return f"{size_bytes / gb:.2f} GB"
    elif size_bytes >= mb:
        return f"{size_bytes / mb:.2f} MB"
    else:
        return f"{size_bytes / kb:.2f} KB"


def _safe(name: str) -> str:
    return name.lower().replace(" ", "_").replace("-", "_").replace(".", "_")