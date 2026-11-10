# dsm-agent
Monitor for Synology DSM systems. Publishes MQTT messages with Home Assistant discovery and takes commands from the shared `upd_control/<host>/command` plane, the same contract as the Pi agents (`pixie-agent`).

## Layout

| Path | Job |
|---|---|
| `dsm-agent.py` | Main agent: MQTT connection, 60 s collection loop, command plane |
| `collectors/dsm_disk_metrics.py` | Volume and USB storage usage |
| `collectors/dsm_service_metrics.py` | `systemctl is-active` for each entry in `services:` |
| `collectors/dsm_sensors.py` | DSM Web API: scrubbing, indexing, Hyper Backup |
| `collectors/dsm_ups.py` | UPS watcher: reads the local NUT server, publishes UPS sensors and events, sends timed shutdown messages, hands power-return to `dsm_power` |
| `controllers/dsm_commands.py` | `check`, `check_raspbian` |
| `controllers/dsm_power.py` | PoE port control on D-Link DGS-1210 switches over SNMPv3: `poe_status`, `poe_restore_plan` (dry run), `poe_cycle` (needs `commands.allow_poe_cycle`) |
| `dsm-docker.py` | Per-container network rates (separate service) |
| `dsm-updates.py`, `chk_raspbian.py` | One-shot update checks, run through the command plane |
| `dsm-units/` | systemd units |

## Install (homestone)

- Install root: `/volume1/IoTdata/dsm-agent`. The units run Package Center's Python 3.14 as root.
- `cp config.example.yaml config.yaml` and fill in every `<set-me>`. `config.yaml` is git-ignored; never commit it.
- `pysnmp` (for `dsm_power`) goes in the agent's own folder, so DSM package updates can't remove it:
  `python3.14 -m pip install --target /volume1/IoTdata/dsm-agent/pylib "pysnmp>=7"`
- Copy the units from `dsm-units/` to `/etc/systemd/system/`, then `sudo systemctl daemon-reload && sudo systemctl enable --now dsm-agent dsm-docker`.

## UPS shutdown and power return

With `ups.enabled: true`, the agent polls NUT on 127.0.0.1:3493 every 5 s. During an outage it:

- publishes events on `ups/ups1/event`;
- sends `shutdown` to each `ups.shed` topic at its time on battery (all at once on low battery or forced shutdown);
- saves its state to `ups_state.json` so a restart mid-outage carries on.

When mains has held for 2 minutes after a long outage, it power-cycles the PoE ports of hosts that halted. It only cuts a port whose link is down, or one that ran past its shutdown time plus `power.link_grace_seconds`.
