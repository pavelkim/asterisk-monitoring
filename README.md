# asterisk-monitoring

Asterisk Server Monitoring via AMI — a Prometheus exporter for Asterisk 18 that
monitors PJSIP trunks, SCCP devices, active channels/calls, and system health.

## Architecture

```
+------------------+       AMI (TCP 5038)      +---------------------+
|  Asterisk 18     | <-----------------------> | asterisk-exporter   |
|  (Rocky Linux 9) |   panoramisk async client |  (Python 3.11)      |
|  chan_sccp        |                           |  pyp8s metrics      |
|  pjsip           |                           |  HTTP :9100         |
+------------------+                           +---------------------+
                                                         |
                                               Prometheus scrape
                                                         |
                                               +------------------+
                                               |  Prometheus      |
                                               |  Alertmanager    |
                                               |  Grafana         |
                                               +------------------+
```

## Quick Start

```bash
# Clone and configure
git clone https://github.com/pavelkim/asterisk-monitoring.git
cd asterisk-monitoring

# Set your AMI secret
export AMI_SECRET=your_secret_here

# Build and run with Docker Compose
docker-compose up -d

# Verify metrics are available
curl http://localhost:9100/metrics
```

## Configuration

All configuration is done via environment variables:

| Variable | Default | Required | Description |
|---|---|---|---|
| `AMI_HOST` | `127.0.0.1` | No | Asterisk AMI host |
| `AMI_PORT` | `5038` | No | Asterisk AMI port |
| `AMI_USER` | `prometheus_monitor` | No | AMI username |
| `AMI_SECRET` | — | **Yes** | AMI password |
| `POLL_INTERVAL` | `30` | No | Polling interval in seconds |
| `PJSIP_TRUNKS` | — | No | Comma-separated PJSIP endpoint names |
| `METRICS_LISTEN_ADDRESS` | `0.0.0.0` | No | Metrics HTTP listen address |
| `METRICS_LISTEN_PORT` | `9100` | No | Metrics HTTP listen port |

## AMI Setup

Copy `config/manager.conf.example` into your Asterisk configuration and reload:

```bash
cp config/manager.conf.example /etc/asterisk/manager.conf
# Edit /etc/asterisk/manager.conf and set a strong secret
asterisk -rx "manager reload"
```

## Metrics Reference

| Metric | Type | Labels | Description |
|---|---|---|---|
| `asterisk_pjsip_trunk_status` | gauge | `trunk` | 1=registered, 0=not registered |
| `asterisk_pjsip_trunk_latency_ms` | gauge | `trunk` | Qualify RTT in milliseconds |
| `asterisk_active_channels` | gauge | — | Number of active channels |
| `asterisk_active_calls` | gauge | — | Number of active calls (bridges) |
| `asterisk_calls_total` | counter | `direction` | Total calls (inbound/outbound/internal) |
| `asterisk_calls_failed_total` | counter | `cause` | Failed calls by hangup cause |
| `asterisk_sccp_devices` | gauge | `device`, `type` | SCCP device status (1=registered) |
| `asterisk_uptime_seconds` | gauge | — | Asterisk uptime in seconds |
| `asterisk_ami_connected` | gauge | — | AMI connection status (1=connected) |

## Prometheus Configuration

```yaml
scrape_configs:
  - job_name: asterisk
    static_configs:
      - targets: ["localhost:9100"]
    scrape_interval: 30s
```

## Alertmanager Rules

```yaml
groups:
  - name: asterisk
    rules:
      - alert: AsteriskAMIDisconnected
        expr: asterisk_ami_connected == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Asterisk AMI connection lost"

      - alert: AsteriskTrunkDown
        expr: asterisk_pjsip_trunk_status == 0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "PJSIP trunk {{ $labels.trunk }} is not registered"

      - alert: AsteriskHighLatency
        expr: asterisk_pjsip_trunk_latency_ms > 200
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "PJSIP trunk {{ $labels.trunk }} latency is high"
```

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally (requires a running Asterisk AMI)
AMI_SECRET=your_secret python -m asterisk_exporter.exporter
```
