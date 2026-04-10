#!/bin/sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PACKAGE_PATH="${1:-$SCRIPT_DIR/awsiot-terminal-agent_0.1.0_arm64.deb}"
PACKAGE_NAME="awsiot-terminal-agent"
SERVICE_NAME="awsiot-mqtt-terminal.service"

if [ "$(id -u)" -ne 0 ]; then
  echo "This script must be run as root." >&2
  exit 1
fi

if [ ! -f "$PACKAGE_PATH" ]; then
  echo "Package not found: $PACKAGE_PATH" >&2
  exit 1
fi

if dpkg-query -W -f='${Status}' "$PACKAGE_NAME" 2>/dev/null | grep -q "install ok installed"; then
  echo "Package already installed, skipping package install: $PACKAGE_NAME"
  exit 0
fi

echo "Installing package: $PACKAGE_PATH"
dpkg -i "$PACKAGE_PATH"

echo "Reloading systemd and enabling service"
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"

echo "Starting service"
systemctl restart "$SERVICE_NAME"

echo "Service status:"
systemctl status "$SERVICE_NAME" --no-pager -l
