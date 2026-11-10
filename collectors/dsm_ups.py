#!/usr/bin/env python3
"""
dsm_ups.py — UPS-1 watcher for dsm-agent.

homestone is the NUT server for UPS-1 (APC NS 1500M2 on USB), so it sees power
events first and stays up longest (DSM waits for low battery). This collector
makes it the power coordinator for the PoE hosts.

It runs in its own thread, polling homestone's own NUT server on 127.0.0.1:3493
every few seconds (the agent's 60 s main loop is too slow for power events). It
speaks the plain NUT text protocol, so it does not depend on DSM shipping upsc.

Publishes:
  state   {base_topic}/{device}/ups       retained, plus HA discovery
  events  ups.event_topic                 NOT retained: ONBATT, ONLINE, LOWBATT,
                                          FSD, COMMBAD, COMMOK, RESTORED
  shed    each ups.shed[].topic           NOT retained, payload "shutdown",
                                          once per outage after N s on battery
                                          (all of them at once on LB/FSD)

When mains has held for ups.restore_after_seconds after a long outage (or one
that reached LB/FSD), it hands off to controllers.dsm_power.restore_ports(),
which power-cycles the PoE ports of hosts that halted.

Outage start, longest stint, fired shed steps and the critical flag persist to
ups.state_file, so an agent restart mid-outage does not reset the clock.

The Pis keep their own upssched 20-minute timers; this does not replace them.
"""

import json
import logging
import os
import socket
import threading
import time
from datetime import datetime, timezone

from controllers import dsm_power

DEFAULTS = {
    "enabled": True,
    "name": "ups1",
    "nut_host": "127.0.0.1",
    "nut_port": 3493,
    "nut_ups": "ups",                     # Synology's fixed UPS name
    "poll_seconds": 5,
    "state_publish_seconds": 30,          # retained state also on any flag change
    "event_topic": "ups/ups1/event",
    "state_file": None,                   # default: ups_state.json next to dsm-agent.py
    "comm_fail_polls": 3,
    "restore_after_seconds": 120,         # mains must hold this long
    "restore_min_outage_seconds": 1140,   # below this nothing was shed
    "shed": [],                           # [{"after_seconds": 1200, "topic": "ups/ha_test/shutdown"}]
    "shed_all_on_critical": True,
}

def _fresh_state():
    return {
        "on_battery_since": None,   # epoch seconds of the current battery stint
        "longest_outage": 0,        # longest stint so far in this outage (flaps)
        "fired": [],                # shed topics already sent this outage
        "critical": False,          # LB or FSD seen this outage
    }


def _cfg(agent):
    cfg = dict(DEFAULTS)
    cfg.update(agent.config.get("ups", {}) or {})
    return cfg


def _state_topic(agent):
    base = agent.config["mqtt"].get("base_topic", "system-sensors/sensor")
    return f"{base}/{agent.device}/ups"


def _iso(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().isoformat()


# ---------- NUT client ----------


def nut_list_vars(host, port, ups, timeout=3.0):
    """Return {var: value} for one UPS using the NUT text protocol.

    Raises on connection errors, ERR replies or a reply without ups.status.
    """
    end_marker = f"END LIST VAR {ups}".encode()
    buf = b""
    with socket.create_connection((host, port), timeout=timeout) as s:
        s.sendall(f"LIST VAR {ups}\n".encode())
        while end_marker not in buf:
            chunk = s.recv(4096)
            if not chunk:
                break
            buf += chunk
            if buf.startswith(b"ERR") or b"\nERR " in buf:
                break
        try:
            s.sendall(b"LOGOUT\n")
        except OSError:
            pass
    return parse_list_vars(buf.decode(errors="replace"), ups)


def parse_list_vars(text, ups):
    prefix = f"VAR {ups} "
    out = {}
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("ERR"):
            raise RuntimeError(f"NUT: {line}")
        if line.startswith(prefix):
            key, _, val = line[len(prefix):].partition(" ")
            val = val.strip()
            if len(val) >= 2 and val[0] == val[-1] == '"':
                val = val[1:-1].replace('\\"', '"').replace("\\\\", "\\")
            out[key] = val
    if "ups.status" not in out:
        raise RuntimeError("NUT reply had no ups.status")
    return out


# ---------- Discovery ----------


def publish_discovery(agent):
    cfg = _cfg(agent)
    if not cfg["enabled"]:
        return
    name = cfg["name"]
    topic = _state_topic(agent)
    label = name.upper().replace("UPS", "UPS-") if name.lower().startswith("ups") else name

    sensors = [
        # key, friendly name, extra
        ("status", f"{label} Status", {"icon": "mdi:power-plug-battery"}),
        ("power", f"{label} Power Source", {"icon": "mdi:transmission-tower"}),
        ("battery_charge", f"{label} Battery Charge",
         {"unit_of_measurement": "%", "device_class": "battery", "state_class": "measurement"}),
        ("battery_runtime", f"{label} Battery Runtime",
         {"unit_of_measurement": "s", "device_class": "duration", "state_class": "measurement"}),
        ("input_voltage", f"{label} Input Voltage",
         {"unit_of_measurement": "V", "device_class": "voltage", "state_class": "measurement"}),
        ("on_battery_since", f"{label} On Battery Since", {"device_class": "timestamp"}),
        ("on_battery_seconds", f"{label} Time On Battery",
         {"unit_of_measurement": "s", "device_class": "duration", "icon": "mdi:timer-sand"}),
    ]
    for key, fname, extra in sensors:
        payload = {
            "name": fname,
            "state_topic": topic,
            "value_template": f"{{{{ value_json.{key} }}}}",
            "unique_id": f"{agent.device}_{name}_{key}",
            "device": agent.sensor_device_info,
        }
        payload.update(extra)
        agent.mqtt.publish(
            f"{agent.discovery_prefix}/sensor/{agent.device}/{name}_{key}/config",
            json.dumps(payload), qos=1, retain=True,
        )

    binaries = [
        ("on_battery", f"{label} On Battery", {"icon": "mdi:battery-alert"}),
        ("nut_ok", f"{label} NUT Link", {"device_class": "connectivity"}),
    ]
    for key, fname, extra in binaries:
        payload = {
            "name": fname,
            "state_topic": topic,
            "value_template": f"{{{{ value_json.{key} }}}}",
            "unique_id": f"{agent.device}_{name}_{key}",
            "device": agent.sensor_device_info,
        }
        payload.update(extra)
        agent.mqtt.publish(
            f"{agent.discovery_prefix}/binary_sensor/{agent.device}/{name}_{key}/config",
            json.dumps(payload), qos=1, retain=True,
        )


# ---------- Monitor thread ----------


class UpsMonitor(threading.Thread):
    def __init__(self, agent):
        super().__init__(name="ups-monitor", daemon=True)
        self.agent = agent
        self.cfg = _cfg(agent)
        self.stop_event = threading.Event()
        self.state_topic = _state_topic(agent)
        self.state_file = self.cfg["state_file"] or os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "ups_state.json"
        )
        self.st = self._load_state()

        self.prev_flags = None        # None until the first good reading
        self.last_vars = {}
        self.nut_ok = True
        self.fail_count = 0
        self.ol_since = None          # wall time mains came back in this outage
        self.last_state_pub = 0.0

    # ----- persistence -----

    def _load_state(self):
        try:
            with open(self.state_file) as f:
                st = json.load(f)
            merged = _fresh_state()
            merged.update({k: st[k] for k in merged if k in st})
            if merged["on_battery_since"]:
                logging.warning(
                    "ups: resuming outage from %s (fired=%s critical=%s)",
                    _iso(merged["on_battery_since"]), merged["fired"], merged["critical"],
                )
            return merged
        except FileNotFoundError:
            return _fresh_state()
        except Exception as e:
            logging.error(f"ups: state file unreadable, starting clean: {e}")
            return _fresh_state()

    def _save_state(self):
        tmp = self.state_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(self.st, f)
            os.replace(tmp, self.state_file)
        except Exception as e:
            logging.error(f"ups: cannot write state file {self.state_file}: {e}")

    # ----- lifecycle -----

    def stop(self):
        self.stop_event.set()

    def run(self):
        logging.info(
            "ups: monitoring %s@%s:%s every %ss",
            self.cfg["nut_ups"], self.cfg["nut_host"], self.cfg["nut_port"], self.cfg["poll_seconds"],
        )
        while not self.stop_event.is_set():
            try:
                v = nut_list_vars(self.cfg["nut_host"], int(self.cfg["nut_port"]), self.cfg["nut_ups"])
                self._on_reading(v)
            except Exception as e:
                self._on_failure(e)
            self.stop_event.wait(float(self.cfg["poll_seconds"]))

    # ----- core logic -----

    def _on_failure(self, err):
        self.fail_count += 1
        if self.fail_count == int(self.cfg["comm_fail_polls"]) and self.nut_ok:
            self.nut_ok = False
            logging.error(f"ups: NUT unreachable: {err}")
            self._event("COMMBAD", detail=str(err))
            self._publish_state(force=True)
        # Keep shedding on schedule if the last good reading said on battery.
        if self.prev_flags and "OB" in self.prev_flags:
            self._run_shed(time.time())

    def _on_reading(self, v):
        now = time.time()
        flags = set(v.get("ups.status", "").split())
        self.last_vars = v

        if not self.nut_ok:
            self.nut_ok = True
            self._event("COMMOK")
        self.fail_count = 0

        first = self.prev_flags is None
        prev = self.prev_flags or set()
        on_batt = "OB" in flags

        if on_batt:
            mains_back_at = self.ol_since
            self.ol_since = None
            if self.st["on_battery_since"] is None:
                self.st["on_battery_since"] = now
                self._save_state()
                self._event("ONBATT")
            elif "OB" not in prev and not first:
                # Flap: mains came back briefly, then failed again. The Pis'
                # upssched timers restart on every ONBATT, so restart ours too,
                # but remember the longest stint for the restore decision.
                stint = (mains_back_at or now) - self.st["on_battery_since"]
                self.st["longest_outage"] = max(self.st["longest_outage"], stint)
                self.st["on_battery_since"] = now
                self._save_state()
                self._event("ONBATT", detail="mains failed again")
        else:
            if "OB" in prev:
                self.ol_since = now
                self._event("ONLINE")
            if "OL" in flags and self.st["on_battery_since"] is not None:
                if self.ol_since is None:      # e.g. agent restarted after mains returned
                    self.ol_since = now
                if now - self.ol_since >= float(self.cfg["restore_after_seconds"]):
                    self._finish_outage()

        for flag, name in (("LB", "LOWBATT"), ("FSD", "FSD")):
            if flag in flags and flag not in prev:
                self.st["critical"] = True
                self._save_state()
                self._event(name)

        self._run_shed(now)

        changed = flags != prev
        self.prev_flags = flags
        self._publish_state(force=changed)

    def _run_shed(self, now):
        since = self.st["on_battery_since"]
        if since is None:
            return
        elapsed = now - since
        dirty = False
        for step in self.cfg.get("shed") or []:
            topic = step.get("topic")
            if not topic or topic in self.st["fired"]:
                continue
            due = elapsed >= float(step.get("after_seconds", 0))
            if due or (self.st["critical"] and self.cfg["shed_all_on_critical"]):
                reason = "critical" if not due else f"{int(elapsed)}s on battery"
                logging.warning(f"ups: shed -> {topic} ({reason})")
                self.agent.mqtt.publish(topic, "shutdown", qos=1, retain=False)
                self.st["fired"].append(topic)
                dirty = True
                self._event("SHED", detail=f"{topic} ({reason})")
        if dirty:
            self._save_state()

    def _finish_outage(self):
        since = self.st["on_battery_since"]
        stint = self.ol_since - since
        outage = max(stint, self.st["longest_outage"])
        critical = self.st["critical"]
        self._event("RESTORED", detail=f"outage {int(outage)}s critical={critical}")

        if outage >= float(self.cfg["restore_min_outage_seconds"]) or critical:
            threading.Thread(
                target=dsm_power.restore_ports,
                args=(self.agent, outage, critical),
                name="ups-restore",
                daemon=True,
            ).start()
        else:
            logging.info(f"ups: outage {int(outage)}s was short; nothing was shed, no PoE cycle")

        self.st = _fresh_state()
        self._save_state()
        self.ol_since = None

    # ----- publishing -----

    def _event(self, name, detail=""):
        v = self.last_vars
        since = self.st["on_battery_since"]
        payload = {
            "event": name,
            "ups": self.cfg["name"],
            "status": v.get("ups.status"),
            "battery_charge": v.get("battery.charge"),
            "battery_runtime": v.get("battery.runtime"),
            "on_battery_seconds": int(time.time() - since) if since else 0,
            "detail": detail,
            "timestamp": datetime.now().astimezone().isoformat(),
        }
        logging.warning(f"ups: event {name} {detail}".rstrip())
        self.agent.mqtt.publish(self.cfg["event_topic"], json.dumps(payload), qos=1, retain=False)

    def _publish_state(self, force=False):
        now = time.time()
        if not force and now - self.last_state_pub < float(self.cfg["state_publish_seconds"]):
            return
        self.last_state_pub = now
        v = self.last_vars
        flags = self.prev_flags or set()
        since = self.st["on_battery_since"]

        def num(key):
            try:
                return float(v[key])
            except (KeyError, ValueError):
                return None

        payload = {
            "status": v.get("ups.status", "unknown") if self.nut_ok else "unknown",
            "power": ("Battery" if "OB" in flags else "Mains" if "OL" in flags else "Unknown")
            if self.nut_ok else "Unknown",
            "battery_charge": num("battery.charge"),
            "battery_runtime": num("battery.runtime"),
            "input_voltage": num("input.voltage"),
            "on_battery": "ON" if "OB" in flags else "OFF",
            "on_battery_since": _iso(since),
            "on_battery_seconds": int(now - since) if since else 0,
            "nut_ok": "ON" if self.nut_ok else "OFF",
        }
        self.agent.mqtt.publish(self.state_topic, json.dumps(payload), qos=1, retain=True)
