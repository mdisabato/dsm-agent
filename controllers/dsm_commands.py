"""
dsm_commands.py — DSM-specific command handlers for the upd_control plane.

Each handler receives:
    payload     (dict)      Full command payload from MQTT
    publish_fn  (callable)  Function to publish status messages back to MQTT

Handlers publish status at start and completion so the control plane
has full lifecycle visibility.

COMMAND_REGISTRY maps command names to handler functions.
dsm_agent.py imports this registry and dispatches accordingly.

Note: No reboot, shutdown, or package install commands — NAS availability
is not something to risk from a dashboard button.
"""

import subprocess
import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Path to dsm-updates.py — same directory as dsm_agent.py
SCRIPT_DIR = "/volume1/IoTdata/dsm-agent"
DSM_UPDATES  = os.path.join(SCRIPT_DIR, "dsm-updates.py")
DSM_RASPBIAN = os.path.join(SCRIPT_DIR, "chk_raspbian.py")
VENV_PYTHON  = "/volume1/@appstore/python314/bin/python3.14"


# ---------------------------------------------------------------------------
# Status helper
# ---------------------------------------------------------------------------

def _status(publish_fn, payload, status, step, message, extra=None):
    """Build and publish a status message."""
    msg = {
        "host":            payload.get("host", "unknown"),
        "command":         payload.get("action", "unknown"),
        "status":          status,
        "step":            step,
        "message":         message,
        "notification_id": payload.get("notification_id", ""),
        "trace":           payload.get("trace", False),
        "timestamp":       datetime.now().isoformat(),
    }
    if extra:
        msg.update(extra)
    publish_fn(msg)


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def handle_check_updates(payload, publish_fn):
    """
    Trigger an update check by running dsm-updates.py directly.
    Results are published to MQTT by dsm-updates.py itself.
    Status messages bookend the run so the control plane knows it started/finished.
    """
    _status(publish_fn, payload, "running", "started", "Update check initiated")

    try:
        result = subprocess.run(
            [VENV_PYTHON, DSM_UPDATES],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode == 0:
            _status(publish_fn, payload, "success", "complete", "Update check complete")
        else:
            _status(publish_fn, payload, "error", "complete",
                    f"Update check failed: {result.stderr.strip()}")

    except subprocess.TimeoutExpired:
        _status(publish_fn, payload, "error", "complete", "Update check timed out")
    except Exception as e:
        logger.error(f"handle_check_updates error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))


def handle_check_raspbian(payload, publish_fn):
    """
    Fetch current Raspberry Pi OS release info and publish to MQTT.
    Results are published to MQTT by chk_raspbian.py itself.
    Status messages bookend the run so the control plane knows it started/finished.
    """
    _status(publish_fn, payload, "running", "started", "Raspbian check initiated")

    try:
        result = subprocess.run(
            [VENV_PYTHON, DSM_RASPBIAN],
            capture_output=True,
            text=True,
            timeout=60,
        )
        if result.returncode == 0:
            _status(publish_fn, payload, "success", "complete", "Raspbian check complete")
        else:
            _status(publish_fn, payload, "error", "complete",
                    f"Raspbian check failed: {result.stderr.strip()}")

    except subprocess.TimeoutExpired:
        _status(publish_fn, payload, "error", "complete", "Raspbian check timed out")
    except Exception as e:
        logger.error(f"handle_check_raspbian error: {e}")
        _status(publish_fn, payload, "error", "complete", str(e))


# ---------------------------------------------------------------------------
# Registry — dsm_agent.py imports this
# ---------------------------------------------------------------------------

COMMAND_REGISTRY = {
    "check":           handle_check_updates,
    "check_raspbian":  handle_check_raspbian,
}