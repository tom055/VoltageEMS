#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR="$(cd "${SCRIPT_PATH%/*}" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

ARCH="${1:-arm64}"
CHECK_MODE="${2:-source}"

EDGE_DIR="$ROOT_DIR/services/awsiot-terminal/edge-device"
EDGE_DEB="$ROOT_DIR/build/awsiot-edge/awsiot-terminal-agent_0.1.0_arm64.deb"
EDGE_INSTALL="$ROOT_DIR/scripts/install-awsiot-deb.sh"
EDGE_TEMPLATE="$ROOT_DIR/scripts/awsiot-edge-deb-template"
EDGE_CONFIG="$EDGE_DIR/awsiot-device.conf"
EDGE_CARGO="$EDGE_DIR/awsiot-terminal-agent-rs/Cargo.toml"
EDGE_MAIN="$EDGE_DIR/awsiot-terminal-agent-rs/src/main.rs"

if [[ "$ARCH" != "arm64" ]]; then
    echo "Skipping AWS IoT edge bundle check for architecture: $ARCH"
    exit 0
fi

if [[ ! -d "$EDGE_DIR" ]]; then
    echo "ERROR: AWS IoT edge directory not found: $EDGE_DIR" >&2
    exit 1
fi

if [[ ! -f "$EDGE_INSTALL" ]]; then
    echo "ERROR: AWS IoT edge install script not found: $EDGE_INSTALL" >&2
    exit 1
fi

case "$CHECK_MODE" in
    source)
        if [[ ! -d "$EDGE_TEMPLATE" ]]; then
            echo "ERROR: AWS IoT edge deb template not found: $EDGE_TEMPLATE" >&2
            exit 1
        fi
        if [[ ! -f "$EDGE_CONFIG" ]]; then
            echo "ERROR: AWS IoT edge config not found: $EDGE_CONFIG" >&2
            exit 1
        fi
        if [[ ! -f "$EDGE_CARGO" ]]; then
            echo "ERROR: AWS IoT edge Cargo.toml not found: $EDGE_CARGO" >&2
            exit 1
        fi
        if [[ ! -f "$EDGE_MAIN" ]]; then
            echo "ERROR: AWS IoT edge source not found: $EDGE_MAIN" >&2
            exit 1
        fi
        echo "AWS IoT edge source check passed for $ARCH"
        echo "  template: $EDGE_TEMPLATE"
        echo "  install script: $EDGE_INSTALL"
        ;;
    bundle)
        if [[ ! -f "$EDGE_DEB" ]]; then
            echo "ERROR: AWS IoT edge deb not found: $EDGE_DEB" >&2
            exit 1
        fi
        echo "AWS IoT edge bundle check passed for $ARCH"
        echo "  deb: $EDGE_DEB"
        echo "  install script: $EDGE_INSTALL"
        ;;
    *)
        echo "ERROR: Unknown check mode: $CHECK_MODE" >&2
        exit 1
        ;;
esac
