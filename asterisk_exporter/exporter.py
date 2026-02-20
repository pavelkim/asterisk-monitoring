"""
Asterisk Prometheus Exporter

Monitors SIP trunks (PJSIP), SCCP devices, active channels/calls,
and system health via the Asterisk Manager Interface (AMI).
"""

import asyncio
import logging
import os
import re
import signal
import sys

import panoramisk
from pyp8s import MetricsHandler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("asterisk_exporter")

AMI_HOST = os.environ.get("AMI_HOST", "127.0.0.1")
AMI_PORT = int(os.environ.get("AMI_PORT", "5038"))
AMI_USER = os.environ.get("AMI_USER", "prometheus_monitor")
AMI_SECRET = os.environ.get("AMI_SECRET", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "30"))
PJSIP_TRUNKS_RAW = os.environ.get("PJSIP_TRUNKS", "")
PJSIP_TRUNKS = [t.strip() for t in PJSIP_TRUNKS_RAW.split(",") if t.strip()]
METRICS_LISTEN_ADDRESS = os.environ.get("METRICS_LISTEN_ADDRESS", "0.0.0.0")
METRICS_LISTEN_PORT = int(os.environ.get("METRICS_LISTEN_PORT", "9100"))


def init_metrics():
    MetricsHandler.init("asterisk_pjsip_trunk_status", "gauge",
                        "PJSIP trunk registration status (1=registered, 0=not)")
    MetricsHandler.init("asterisk_pjsip_trunk_latency_ms", "gauge",
                        "PJSIP trunk qualify RTT in milliseconds")
    MetricsHandler.init("asterisk_active_channels", "gauge",
                        "Number of active channels")
    MetricsHandler.init("asterisk_active_calls", "gauge",
                        "Number of active calls (bridges)")
    MetricsHandler.init("asterisk_calls_total", "counter",
                        "Total calls by direction (inbound/outbound/internal)")
    MetricsHandler.init("asterisk_calls_failed_total", "counter",
                        "Failed calls by hangup cause")
    MetricsHandler.init("asterisk_sccp_devices", "gauge",
                        "SCCP device registration status")
    MetricsHandler.init("asterisk_uptime_seconds", "gauge",
                        "Asterisk uptime in seconds")
    MetricsHandler.init("asterisk_ami_connected", "gauge",
                        "AMI connection status (1=connected, 0=disconnected)")


class AsteriskExporter:
    def __init__(self):
        self._manager = None
        self._connected = False
        self._loop = None

    def _build_manager(self):
        return panoramisk.Manager(
            host=AMI_HOST,
            port=AMI_PORT,
            username=AMI_USER,
            secret=AMI_SECRET,
            loop=self._loop,
            on_login=self._on_login,
            on_connect=self._on_connect,
            on_disconnect=self._on_disconnect,
        )

    def _on_connect(self, manager, future):
        logger.info("AMI connection established")

    def _on_login(self, manager, future):
        logger.info("AMI login successful")
        self._connected = True
        MetricsHandler.set("asterisk_ami_connected", 1)

    def _on_disconnect(self, manager, future):
        logger.warning("AMI disconnected")
        self._connected = False
        MetricsHandler.set("asterisk_ami_connected", 0)

    def _register_events(self):
        self._manager.register_event("Newchannel", self._handle_newchannel)
        self._manager.register_event("Hangup", self._handle_hangup)
        self._manager.register_event("PeerStatus", self._handle_peer_status)
        self._manager.register_event("Registry", self._handle_registry)

    def _handle_newchannel(self, manager, event):
        context = event.get("Context", "")
        channel = event.get("Channel", "")
        if "from-trunk" in context or "from-pstn" in context:
            direction = "inbound"
        elif "to-trunk" in context or "to-pstn" in context:
            direction = "outbound"
        else:
            direction = "internal"
        MetricsHandler.inc("asterisk_calls_total", 1, direction=direction)
        logger.debug("Newchannel event: channel=%s direction=%s", channel, direction)

    def _handle_hangup(self, manager, event):
        cause = event.get("Cause", "0")
        cause_txt = event.get("Cause-txt", "Unknown")
        try:
            cause_code = int(cause)
        except (ValueError, TypeError):
            cause_code = 0
        # Only count non-normal hangups as failures.
        # Cause 16 = Normal call clearing (success), 0 = unset.
        if cause_code not in (0, 16):
            cause_label = re.sub(r"[^a-zA-Z0-9_]", "_", cause_txt).lower()
            MetricsHandler.inc("asterisk_calls_failed_total", 1, cause=cause_label)
            logger.debug("Hangup failure: cause=%s (%s)", cause, cause_txt)

    def _handle_peer_status(self, manager, event):
        peer = event.get("Peer", "")
        status = event.get("PeerStatus", "")
        logger.debug("PeerStatus event: peer=%s status=%s", peer, status)

    def _handle_registry(self, manager, event):
        channel_driver = event.get("ChannelDriver", "")
        domain = event.get("Domain", "")
        status = event.get("Status", "")
        logger.debug("Registry event: driver=%s domain=%s status=%s",
                     channel_driver, domain, status)

    async def _poll_pjsip_trunks(self):
        if not PJSIP_TRUNKS:
            return
        for trunk in PJSIP_TRUNKS:
            try:
                response = await self._manager.send_action({
                    "Action": "PJSIPShowEndpoint",
                    "Endpoint": trunk,
                })
                registered = 0
                latency_ms = 0.0
                if response:
                    events = response if isinstance(response, list) else [response]
                    for ev in events:
                        ev_name = ev.get("Event", "")
                        if ev_name == "ContactStatusDetail":
                            uri_status = ev.get("Status", "")
                            if uri_status.lower() == "reachable":
                                registered = 1
                            rtt = ev.get("RoundtripUsec", "0")
                            try:
                                latency_ms = float(rtt) / 1000.0
                            except (ValueError, TypeError):
                                latency_ms = 0.0
                MetricsHandler.set("asterisk_pjsip_trunk_status", registered, trunk=trunk)
                MetricsHandler.set("asterisk_pjsip_trunk_latency_ms", latency_ms, trunk=trunk)
                logger.debug("PJSIP trunk %s: registered=%d latency_ms=%.3f",
                             trunk, registered, latency_ms)
            except Exception as exc:
                logger.warning("Failed to poll PJSIP trunk %s: %s", trunk, exc)
                MetricsHandler.set("asterisk_pjsip_trunk_status", 0, trunk=trunk)

    async def _poll_active_channels(self):
        try:
            response = await self._manager.send_action({"Action": "CoreShowChannels"})
            count = 0
            if response:
                events = response if isinstance(response, list) else [response]
                count = sum(
                    1 for ev in events if ev.get("Event") == "CoreShowChannel"
                )
            MetricsHandler.set("asterisk_active_channels", count)
            logger.debug("Active channels: %d", count)
        except Exception as exc:
            logger.warning("Failed to poll active channels: %s", exc)

    async def _poll_active_calls(self):
        try:
            response = await self._manager.send_action({"Action": "BridgeList"})
            count = 0
            if response:
                events = response if isinstance(response, list) else [response]
                count = sum(
                    1 for ev in events if ev.get("Event") == "BridgeListItem"
                )
            MetricsHandler.set("asterisk_active_calls", count)
            logger.debug("Active calls: %d", count)
        except Exception as exc:
            logger.warning("Failed to poll active calls: %s", exc)

    async def _poll_sccp_devices(self):
        try:
            response = await self._manager.send_action({
                "Action": "Command",
                "Command": "sccp show devices",
            })
            output = ""
            if response:
                if isinstance(response, list):
                    for ev in response:
                        output += ev.get("Output", "")
                else:
                    output = response.get("Output", "")
            for line in output.splitlines():
                line = line.strip()
                if not line or line.startswith("Name") or line.startswith("---"):
                    continue
                parts = line.split()
                if len(parts) >= 3:
                    device_name = parts[0]
                    device_type = parts[1]
                    reg_status = parts[2].lower()
                    status_val = 1 if reg_status == "registered" else 0
                    MetricsHandler.set("asterisk_sccp_devices", status_val,
                                       device=device_name, type=device_type)
                    logger.debug("SCCP device %s type=%s status=%d",
                                 device_name, device_type, status_val)
        except Exception as exc:
            logger.warning("Failed to poll SCCP devices: %s", exc)

    async def _poll_system_uptime(self):
        try:
            response = await self._manager.send_action({"Action": "CoreStatus"})
            uptime_seconds = 0
            if response:
                ev = response if not isinstance(response, list) else (
                    response[0] if response else {}
                )
                uptime_str = ev.get("CoreUptime", "") or ev.get("Uptime", "")
                uptime_seconds = self._parse_uptime(uptime_str)
            MetricsHandler.set("asterisk_uptime_seconds", uptime_seconds)
            logger.debug("Asterisk uptime: %d seconds", uptime_seconds)
        except Exception as exc:
            logger.warning("Failed to poll system uptime: %s", exc)

    @staticmethod
    def _parse_uptime(uptime_str):
        """Parse Asterisk uptime string like '1 day, 2 hours, 3 minutes, 4 seconds'."""
        if not uptime_str:
            return 0
        total = 0
        patterns = [
            (r"(\d+)\s+day", 86400),
            (r"(\d+)\s+hour", 3600),
            (r"(\d+)\s+minute", 60),
            (r"(\d+)\s+second", 1),
        ]
        for pattern, multiplier in patterns:
            match = re.search(pattern, uptime_str, re.IGNORECASE)
            if match:
                total += int(match.group(1)) * multiplier
        return total

    async def _poll_loop(self):
        while True:
            if self._connected:
                logger.info("Running poll cycle")
                await asyncio.gather(
                    self._poll_pjsip_trunks(),
                    self._poll_active_channels(),
                    self._poll_active_calls(),
                    self._poll_sccp_devices(),
                    self._poll_system_uptime(),
                    return_exceptions=True,
                )
            await asyncio.sleep(POLL_INTERVAL)

    async def run(self):
        self._loop = asyncio.get_running_loop()
        self._manager = self._build_manager()
        self._register_events()

        MetricsHandler.set("asterisk_ami_connected", 0)

        self._manager.connect()

        await self._poll_loop()

    def stop(self):
        if self._manager:
            self._manager.close()


def main():
    if not AMI_SECRET:
        logger.error("AMI_SECRET environment variable is required")
        sys.exit(1)

    init_metrics()

    MetricsHandler.serve(
        listen_address=METRICS_LISTEN_ADDRESS,
        listen_port=METRICS_LISTEN_PORT,
    )
    logger.info("Metrics server started on %s:%d",
                METRICS_LISTEN_ADDRESS, METRICS_LISTEN_PORT)

    exporter = AsteriskExporter()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    task = None

    def _shutdown(signum, frame):
        logger.info("Received signal %d, shutting down", signum)
        exporter.stop()
        if task is not None:
            loop.call_soon_threadsafe(task.cancel)
        loop.call_soon_threadsafe(loop.stop)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    task = loop.create_task(exporter.run())
    try:
        loop.run_forever()
    finally:
        loop.close()


if __name__ == "__main__":
    main()
