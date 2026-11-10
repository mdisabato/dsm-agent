"""
dsm_power.py — PoE port control for dsm-agent (SNMPv3 to the D-Link DGS-1210s).

Two users:
  1. collectors/dsm_ups.py calls restore_ports() when mains has held after a
     long outage, so hosts that halted on UPS-1's battery boot again. homestone
     outlives every PoE host, so it can also cycle ha-prod's own port.
  2. make_commands(config) adds handlers to the upd_control plane:
       poe_status        read PoE admin + link for every configured port (no changes)
       poe_restore_plan  dry run: what restore_ports() would do for a given outage
       poe_cycle         power-cycle one configured port (needs commands.allow_poe_cycle)

SNMP (same as the Node-RED "PoE SNMP Controls" flow):
  pethPsePortAdminEnable  1.3.6.1.2.1.105.1.1.1.3.1.<port>   1 = on, 2 = off
  ifOperStatus            1.3.6.1.2.1.2.2.1.8.<port>          1 = up, 2 = down
  SNMPv3 authNoPriv, SHA.

Safety: a port is only cut when its link is down (host halted), or when the
outage ran past that host's shed time plus power.link_grace_seconds and
power.assume_halted_after_grace is true. A port the outage never reached
(outage < shed_after_seconds, no LB/FSD) is never touched.

Needs pysnmp >= 7 in <agent root>/pylib (pip install --target pylib pysnmp). Imported
lazily, so the rest of dsm-agent runs without it.
"""

import asyncio
import logging
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

POE_ADMIN_OID = "1.3.6.1.2.1.105.1.1.1.3.1.{port}"
IF_OPER_OID = "1.3.6.1.2.1.2.2.1.8.{port}"

_cycle_lock = threading.Lock()   # one restore / cycle sequence at a time


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _power_cfg(config):
    p = dict(config.get("power", {}) or {})
    p.setdefault("off_seconds", 10)
    p.setdefault("link_grace_seconds", 120)
    p.setdefault("assume_halted_after_grace", True)
    p.setdefault("ports", [])
    p.setdefault("switches", {})
    p.setdefault("snmp", {})
    return p


def _find_port(pcfg, payload):
    """Match a command payload to a configured port by name, or switch+port."""
    name = (payload.get("target") or payload.get("name") or "").lower()
    sw = str(payload.get("switch", "")).lower()
    num = payload.get("port")
    for p in pcfg["ports"]:
        if name and p.get("name", "").lower() == name:
            return p
        if sw and num is not None and str(p.get("switch", "")).lower() == sw and int(p["port"]) == int(num):
            return p
    return None


# ---------------------------------------------------------------------------
# SNMP
# ---------------------------------------------------------------------------

def _use_pylib():
    """pysnmp lives in <agent root>/pylib (pip install --target), outside
    Synology's python314 package, so DSM package updates can't wipe it."""
    import os
    import sys
    lib = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "pylib")
    if os.path.isdir(lib) and lib not in sys.path:
        sys.path.append(lib)


def _snmp(pcfg, switch, oid, value=None):
    """GET (value None) or SET an Integer. Returns the integer read back."""
    _use_pylib()
    from pysnmp.hlapi.v3arch.asyncio import (
        SnmpEngine, UsmUserData, UdpTransportTarget, ContextData,
        ObjectType, ObjectIdentity, get_cmd, set_cmd,
        usmHMACSHAAuthProtocol, usmNoPrivProtocol,
    )
    from pysnmp.proto.rfc1902 import Integer

    host = pcfg["switches"].get(str(switch).lower())
    if not host:
        raise ValueError(f"unknown switch '{switch}'")
    snmp = pcfg["snmp"]
    if str(snmp.get("auth_protocol", "SHA")).upper() != "SHA":
        raise ValueError("only SHA auth is implemented")
    if not snmp.get("auth_key") or snmp.get("auth_key") == "SET-ME":
        raise ValueError("power.snmp.auth_key is not set in config.yaml")

    async def go():
        engine = SnmpEngine()
        try:
            target = await UdpTransportTarget.create(
                (host, 161),
                timeout=float(snmp.get("timeout", 2)),
                retries=int(snmp.get("retries", 1)),
            )
            user = UsmUserData(
                snmp.get("username", "iotuser"),
                authKey=snmp["auth_key"],
                authProtocol=usmHMACSHAAuthProtocol,
                privProtocol=usmNoPrivProtocol,
            )
            obj = ObjectType(ObjectIdentity(oid)) if value is None \
                else ObjectType(ObjectIdentity(oid), Integer(value))
            cmd = get_cmd if value is None else set_cmd
            return await cmd(engine, user, target, ContextData(), obj)
        finally:
            try:
                engine.close_dispatcher()
            except Exception:
                pass

    err_ind, err_stat, err_idx, binds = asyncio.run(go())
    if err_ind:
        raise RuntimeError(f"SNMP {host}: {err_ind}")
    if err_stat:
        raise RuntimeError(f"SNMP {host}: {err_stat.prettyPrint()} at {err_idx}")
    return int(binds[0][1])


def link_state(pcfg, switch, port):
    """'up', 'down' or 'unknown' (SNMP failed)."""
    try:
        v = _snmp(pcfg, switch, IF_OPER_OID.format(port=int(port)))
        return "up" if v == 1 else "down"
    except Exception as e:
        logger.error(f"link_state {switch}{port}: {e}")
        return "unknown"


def poe_state(pcfg, switch, port):
    """'on', 'off' or 'unknown'."""
    try:
        v = _snmp(pcfg, switch, POE_ADMIN_OID.format(port=int(port)))
        return "on" if v == 1 else "off"
    except Exception as e:
        logger.error(f"poe_state {switch}{port}: {e}")
        return "unknown"


def cycle_port(pcfg, switch, port):
    """PoE off, wait, PoE on. Always tries to turn the port back on."""
    oid = POE_ADMIN_OID.format(port=int(port))
    _snmp(pcfg, switch, oid, 2)
    try:
        time.sleep(float(pcfg["off_seconds"]))
    finally:
        _snmp(pcfg, switch, oid, 1)


# ---------------------------------------------------------------------------
# Restore after an outage
# ---------------------------------------------------------------------------

def plan_restore(pcfg, outage_secs, critical, read_links=True):
    """Decide, per configured port, whether to cycle it. Returns a list of dicts."""
    plan = []
    grace = float(pcfg["link_grace_seconds"])
    for p in pcfg["ports"]:
        shed_after = float(p.get("shed_after_seconds", 0))
        reached = critical or outage_secs >= shed_after
        link = link_state(pcfg, p["switch"], p["port"]) if (reached and read_links) else "not read"
        if not reached:
            action, why = "skip", f"outage {int(outage_secs)}s < shed {int(shed_after)}s"
        elif link == "down":
            action, why = "cycle", "link down (host halted)"
        elif (pcfg["assume_halted_after_grace"] and not critical
              and outage_secs >= shed_after + grace and link != "unknown"):
            action, why = "cycle", f"outage passed shed+grace ({int(shed_after + grace)}s); link {link}"
        else:
            action, why = "skip", f"link {link}; not sure the host halted"
        plan.append({"name": p.get("name"), "switch": p["switch"], "port": p["port"],
                     "link": link, "action": action, "why": why})
    return plan


def restore_ports(agent, outage_secs, critical):
    """Called by dsm_ups when mains has held after an outage."""
    pcfg = _power_cfg(agent.config)
    publish = agent.publish_command_status
    base = {"host": agent.device, "command": "ups_restore", "notification_id": "", "trace": False}

    def status(st, step, msg):
        publish({**base, "status": st, "step": step, "message": msg,
                 "timestamp": datetime.now().isoformat()})

    if not pcfg["ports"]:
        logger.warning("ups_restore: no power.ports configured")
        return

    with _cycle_lock:
        status("running", "started",
               f"Mains back after {int(outage_secs) // 60} min (critical={critical}); checking PoE hosts")
        plan = plan_restore(pcfg, outage_secs, critical)
        done, failed = [], []
        for item in plan:
            logger.warning(f"ups_restore: {item['name']} {item['switch']}{item['port']}: "
                           f"{item['action']} ({item['why']})")
            if item["action"] != "cycle":
                continue
            try:
                cycle_port(pcfg, item["switch"], item["port"])
                done.append(item["name"])
            except Exception as e:
                logger.error(f"ups_restore: cycle {item['name']} failed: {e}")
                failed.append(item["name"])
        msg = f"Cycled: {', '.join(done) or 'none'}"
        if failed:
            msg += f"; FAILED: {', '.join(failed)}"
        status("error" if failed else "success", "complete", msg)


# ---------------------------------------------------------------------------
# Control-plane commands
# ---------------------------------------------------------------------------

def make_commands(config):
    pcfg = _power_cfg(config)
    allow_cycle = bool(config.get("commands", {}).get("allow_poe_cycle", False))

    def _status(publish_fn, payload, st, step, msg):
        publish_fn({
            "host": payload.get("host", "unknown"),
            "command": payload.get("action", "unknown"),
            "status": st, "step": step, "message": msg,
            "notification_id": payload.get("notification_id", ""),
            "trace": payload.get("trace", False),
            "timestamp": datetime.now().isoformat(),
        })

    def handle_poe_status(payload, publish_fn):
        _status(publish_fn, payload, "running", "started", "Reading PoE ports")
        parts = []
        for p in pcfg["ports"]:
            parts.append(f"{p.get('name')} {p['switch']}{p['port']}: "
                         f"poe {poe_state(pcfg, p['switch'], p['port'])}, "
                         f"link {link_state(pcfg, p['switch'], p['port'])}")
        _status(publish_fn, payload, "success", "complete", "; ".join(parts) or "no ports configured")

    def handle_poe_restore_plan(payload, publish_fn):
        outage = float(payload.get("outage_seconds", 1500))
        critical = bool(payload.get("critical", False))
        _status(publish_fn, payload, "running", "started", f"Dry run for a {int(outage)}s outage")
        plan = plan_restore(pcfg, outage, critical)
        msg = "; ".join(f"{i['name']}: {i['action']} ({i['why']})" for i in plan)
        _status(publish_fn, payload, "success", "complete", f"DRY RUN — {msg}")

    def handle_poe_cycle(payload, publish_fn):
        if not allow_cycle:
            _status(publish_fn, payload, "error", "complete", "poe_cycle disabled (commands.allow_poe_cycle)")
            return
        p = _find_port(pcfg, payload)
        if not p:
            _status(publish_fn, payload, "error", "complete", "Port not in power.ports")
            return
        if not _cycle_lock.acquire(blocking=False):
            _status(publish_fn, payload, "error", "complete", "Another PoE cycle is running")
            return
        try:
            _status(publish_fn, payload, "running", "started", f"Cycling {p.get('name')} ({p['switch']}{p['port']})")
            cycle_port(pcfg, p["switch"], p["port"])
            _status(publish_fn, payload, "success", "complete", f"Cycled {p.get('name')}")
        except Exception as e:
            _status(publish_fn, payload, "error", "complete", f"Cycle failed: {e}")
        finally:
            _cycle_lock.release()

    return {
        "poe_status": handle_poe_status,
        "poe_restore_plan": handle_poe_restore_plan,
        "poe_cycle": handle_poe_cycle,
    }
