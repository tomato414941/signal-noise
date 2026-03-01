#!/bin/bash
# Collect all signal-noise data
# Run by cron: 0 3 * * * /home/dev/projects/signal-noise/scripts/collect_all.sh

set -euo pipefail

cd /home/dev/projects/signal-noise
source .venv/bin/activate

python3 -m signal_noise collect 2>&1
