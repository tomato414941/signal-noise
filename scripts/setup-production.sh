#!/usr/bin/env bash
# Bootstrap a fresh VPS for signal-noise production.
# Run as 'dev' user after initial SSH + Tailscale setup.
set -euo pipefail

echo "=== signal-noise production setup ==="

# System packages
sudo apt-get update -qq
sudo apt-get install -y -qq python3.12 python3.12-venv python3.12-dev git dnsutils > /dev/null 2>&1
echo "[1/5] System packages installed"

# Project
mkdir -p ~/projects ~/.secrets
cd ~/projects
if [ ! -d signal-noise ]; then
    git clone git@github.com:tomato414941/signal-noise.git
fi
cd signal-noise
echo "[2/5] Repository cloned"

# Python venv
python3.12 -m venv .venv
.venv/bin/pip install --upgrade pip -q
.venv/bin/pip install -e . -q
echo "[3/5] Python venv ready"

# Manifest + data dirs
.venv/bin/python -m signal_noise rebuild-manifest
mkdir -p data/cache data/raw
echo "[4/5] Manifest built: $(.venv/bin/python -m signal_noise count 2>&1)"

# systemd
sudo cp scripts/signal-noise.service /etc/systemd/system/
sudo cp scripts/signal-noise-scheduler.service /etc/systemd/system/
sudo systemctl daemon-reload
echo "[5/5] systemd units installed"

echo ""
echo "=== Setup complete ==="
echo "Next steps:"
echo "  1. Copy secrets:  scp dev@alpha-os:~/.secrets/{fred,eia,bea,finnhub,binance,sonitus} ~/.secrets/"
echo "  2. chmod 600 ~/.secrets/*"
echo "  3. Copy DB:       scp dev@alpha-os:~/projects/signal-noise/data/signals.db ~/projects/signal-noise/data/"
echo "  4. Start:         sudo systemctl enable --now signal-noise signal-noise-scheduler"
echo "  5. Tailscale:     sudo tailscale serve --bg --yes 127.0.0.1:8000"
echo "  6. Verify:        curl -s http://127.0.0.1:8000/health"
