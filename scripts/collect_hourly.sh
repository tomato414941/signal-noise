#!/bin/bash
# Collect hourly (snapshot) signals only
# Run by cron: 5 * * * * .../collect_hourly.sh

set -euo pipefail

cd /home/trading/projects/signal-noise
source .venv/bin/activate

python3 -m signal_noise collect --frequency hourly 2>&1
