#!/usr/bin/env python3

import logging
import requests
from datetime import datetime


# ---------- Discovery ----------


def publish_discovery(agent):
    """Publish HA discovery for DSM-specific sensors."""

    sensors = [
        ("dsm_scrubbing",    "Disk Scrubbing",    "{{ value_json.dsm_scrubbing }}",    "harddisk-plus"),
        ("dsm_indexing",     "Disk Indexing",      "{{ value_json.dsm_indexing }}",     "file-search"),
        ("dsm_hyper_backup", "Hyper Backup",       "{{ value_json.dsm_hyper_backup }}", "backup-restore"),
    ]

    for key, name, value_template, icon in sensors:
        topic = f"{agent.discovery_prefix}/sensor/{agent.device}/{key}/config"
        payload = {
            "name":           name,
            "state_topic":    agent.state_topic,
            "value_template": value_template,
            "unique_id":      f"{agent.device}_{key}",
            "device":         agent.sensor_device_info,
            "icon":           f"mdi:{icon}",
        }
        agent.mqtt.publish(topic, __import__('json').dumps(payload), qos=1, retain=True)


# ---------- Collection ----------


def collect(agent):
    """Collect DSM-specific sensor data via DSM HTTP API."""
    dsm_cfg = agent.config.get("dsm", {})
    data = {
        "dsm_scrubbing":    "unknown",
        "dsm_indexing":     "unknown",
        "dsm_hyper_backup": "unknown",
    }

    sid = _login(dsm_cfg)
    if not sid:
        logging.error("DSM login failed — sensor data unavailable")
        return data

    try:
        scrubbing, indexing = _get_process_status(dsm_cfg, sid)
        data["dsm_scrubbing"] = "active" if scrubbing else "idle"
        data["dsm_indexing"]  = "active" if indexing  else "idle"
    except Exception as e:
        logging.error(f"Failed to get DSM process status: {e}")

    try:
        backup_status = _get_backup_status(dsm_cfg, sid)
        data["dsm_hyper_backup"] = backup_status
    except Exception as e:
        logging.error(f"Failed to get Hyper Backup status: {e}")

    return data


# ---------- DSM API helpers ----------


def _login(dsm_cfg: dict) -> str | None:
    """Login to DSM and return session ID."""
    try:
        response = requests.get(
            f"http://{dsm_cfg['dsm_ip']}:{dsm_cfg['dsm_port']}/webapi/auth.cgi",
            params={
                "api":     "SYNO.API.Auth",
                "version": "3",
                "method":  "login",
                "account": dsm_cfg["dsm_username"],
                "passwd":  dsm_cfg["dsm_password"],
                "session": "dsm_sensors",
                "format":  "sid",
            },
            timeout=10,
        )
        return response.json().get("data", {}).get("sid")
    except Exception as e:
        logging.error(f"DSM login failed: {e}")
        return None


def _get_backup_status(dsm_cfg: dict, sid: str) -> str:
    """Check Hyper Backup task status. Returns 'active', 'idle', or 'unknown'."""
    try:
        response = requests.get(
            f"http://{dsm_cfg['dsm_ip']}:{dsm_cfg['dsm_port']}/webapi/entry.cgi",
            params={
                "api":     "SYNO.Backup.Task",
                "version": "1",
                "method":  "list",
                "_sid":    sid,
            },
            timeout=10,
        )
        data = response.json()

        if not data.get("success"):
            logging.error(f"Hyper Backup API error: {data.get('error', {})}")
            return "unknown"

        tasks = data.get("data", {}).get("task_list", [])
        for task in tasks:
            status = task.get("status", "").lower()
            if status in ("backup", "backupable", "running"):
                return "active"

        return "idle"
    except Exception as e:
        logging.error(f"Failed to query Hyper Backup API: {e}")
        return "unknown"


def _get_process_status(dsm_cfg: dict, sid: str) -> tuple[bool, bool]:
    """Return (scrubbing, indexing) booleans from DSM process list."""
    response = requests.get(
        f"http://{dsm_cfg['dsm_ip']}:{dsm_cfg['dsm_port']}/webapi/entry.cgi",
        params={
            "api":     "SYNO.Core.System.Process",
            "version": "1",
            "method":  "list",
            "_sid":    sid,
        },
        timeout=10,
    )
    data = response.json()

    scrubbing = False
    indexing  = False

    if data.get("success"):
        processes = data.get("data", {}).get("process", [])

        index_keywords = ["fileindexd", "synoindexd", "synoindexscand", "synoindexworkerd"]
        raid_keywords  = ["md2_raid6", "md2_resync"]

        index_procs = [p for p in processes if any(k in p["command"].lower() for k in index_keywords)]
        raid_procs  = [p for p in processes if any(k in p["command"].lower() for k in raid_keywords)]

        indexing = len(index_procs) > 0

        has_raid6  = any("raid6"  in p["command"].lower() for p in raid_procs)
        has_resync = any("resync" in p["command"].lower() for p in raid_procs)
        scrubbing  = has_raid6 and has_resync

        for proc in raid_procs:
            if proc.get("status") == "R" or proc.get("cpu", 0) > 0:
                scrubbing = True
                break

    return scrubbing, indexing
