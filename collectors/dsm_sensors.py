#!/usr/bin/env python3
"""DSM activity sensors: disk scrubbing, disk indexing and Hyper Backup.

Scrubbing comes from the kernel's md sync state (/sys/block/md*/md/sync_action),
not from the process list: the md2_raid6 kernel thread uses CPU on every
write, so CPU time there says nothing about a scrub.

Indexing and Hyper Backup come from the DSM web API. The agent keeps one DSM
session for its whole life, logs in again only when DSM rejects it, and logs
out on shutdown. A failed reading keeps the last good value until it has
failed `fail_limit` cycles in a row (default 3); only then does the sensor
report "unknown". A failed call is never reported as "idle".
"""

import glob
import json
import logging

import requests


SESSION = "dsm_sensors"
# DSM auth error codes that mean "this sid is no good, log in again"
_SESSION_ERRORS = {105, 106, 107, 119}

_sid = None
_last = {}   # key -> last good value
_fails = {}  # key -> consecutive failed cycles


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
        agent.mqtt.publish(topic, json.dumps(payload), qos=1, retain=True)


# ---------- Collection ----------


def collect(agent):
    """Collect DSM-specific sensor data. Values are 'active', 'idle' or 'unknown'."""
    dsm_cfg = agent.config.get("dsm", {})
    limit = int(dsm_cfg.get("fail_limit", 3))

    readings = {
        "dsm_scrubbing":    _get_scrub_status(),
        "dsm_indexing":     _get_index_status(dsm_cfg),
        "dsm_hyper_backup": _get_backup_status(dsm_cfg),
    }
    return {key: _debounce(key, value, limit) for key, value in readings.items()}


def close(agent):
    """Log the DSM session out. Called once when the agent stops."""
    global _sid
    if not _sid:
        return
    dsm_cfg = agent.config.get("dsm", {})
    try:
        requests.get(
            f"{_base(dsm_cfg)}/auth.cgi",
            params={"api": "SYNO.API.Auth", "version": "3", "method": "logout",
                    "session": SESSION, "_sid": _sid},
            timeout=5,
        )
    except Exception as e:
        logging.warning(f"DSM logout failed: {e}")
    _sid = None


def _debounce(key: str, value, limit: int) -> str:
    """Hold the last good value through short failures."""
    if value is not None:
        if _fails.get(key):
            logging.info(f"{key}: reading recovered after {_fails[key]} failed cycle(s)")
        _fails[key] = 0
        _last[key] = value
        return value
    _fails[key] = _fails.get(key, 0) + 1
    if _fails[key] >= limit or key not in _last:
        return "unknown"
    return _last[key]


# ---------- Readers (each returns a value, or None on failure) ----------


def _get_scrub_status():
    """'active' while any md array is checking, repairing, resyncing or recovering."""
    try:
        files = glob.glob("/sys/block/md*/md/sync_action")
        if not files:
            logging.error("Scrub status: no md sync_action files found")
            return None
        actions = {}
        for path in files:
            with open(path) as f:
                actions[path.split("/")[3]] = f.read().strip()
        busy = {md: a for md, a in actions.items() if a not in ("idle", "frozen")}
        return "active" if busy else "idle"
    except Exception as e:
        logging.error(f"Scrub status: {e}")
        return None


def _get_index_status(dsm_cfg: dict):
    data = _api(dsm_cfg, "SYNO.Core.System.Process", "list")
    if data is None:
        return None
    keywords = ("fileindexd", "synoindexd", "synoindexscand", "synoindexworkerd")
    procs = data.get("process", [])
    indexing = any(any(k in p.get("command", "").lower() for k in keywords) for p in procs)
    return "active" if indexing else "idle"


def _get_backup_status(dsm_cfg: dict):
    """'active' when any Hyper Backup task has a status other than 'none'.

    Idle tasks report state 'backupable' and status 'none'.
    """
    data = _api(dsm_cfg, "SYNO.Backup.Task", "list")
    if data is None:
        return None
    for task in data.get("task_list", []):
        if (task.get("status") or "none").lower() != "none":
            return "active"
    return "idle"


# ---------- DSM API helpers ----------


def _base(dsm_cfg: dict) -> str:
    return f"http://{dsm_cfg['dsm_ip']}:{dsm_cfg['dsm_port']}/webapi"


def _login(dsm_cfg: dict):
    """Log in and return a session ID, or None."""
    try:
        response = requests.get(
            f"{_base(dsm_cfg)}/auth.cgi",
            params={
                "api":     "SYNO.API.Auth",
                "version": "3",
                "method":  "login",
                "account": dsm_cfg["dsm_username"],
                "passwd":  dsm_cfg["dsm_password"],
                "session": SESSION,
                "format":  "sid",
            },
            timeout=10,
        )
        body = response.json()
        sid = body.get("data", {}).get("sid")
        if not sid:
            logging.error(f"DSM login refused: {body.get('error', {})}")
        return sid
    except Exception as e:
        logging.error(f"DSM login failed: {e}")
        return None


def _api(dsm_cfg: dict, api: str, method: str, version: str = "1"):
    """Call a DSM API with the shared session. Returns the 'data' dict, or None.

    Logs in when there is no session, and logs in again (once) when DSM says
    the session is no longer valid.
    """
    global _sid
    for attempt in (1, 2):
        if not _sid:
            _sid = _login(dsm_cfg)
            if not _sid:
                return None
        try:
            response = requests.get(
                f"{_base(dsm_cfg)}/entry.cgi",
                params={"api": api, "version": version, "method": method, "_sid": _sid},
                timeout=10,
            )
            body = response.json()
        except Exception as e:
            logging.error(f"{api}.{method}: request failed (attempt {attempt}): {e}")
            continue
        if body.get("success"):
            return body.get("data", {})
        code = (body.get("error") or {}).get("code")
        logging.error(f"{api}.{method}: DSM error {code} (attempt {attempt}): {body.get('error', {})}")
        if code in _SESSION_ERRORS:
            _sid = None  # log in again on the retry
    return None
