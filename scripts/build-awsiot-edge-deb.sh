#!/usr/bin/env bash
set -euo pipefail

SCRIPT_PATH="${BASH_SOURCE[0]:-$0}"
SCRIPT_DIR="$(cd "${SCRIPT_PATH%/*}" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

EDGE_DIR="$ROOT_DIR/services/awsiot-terminal/edge-device"
TEMPLATE_DIR="$ROOT_DIR/scripts/awsiot-edge-deb-template"
BUILD_ROOT="$ROOT_DIR/build/awsiot-edge"
STAGE_DIR="$BUILD_ROOT/awsiot-deb-arm64"
OUTPUT_DEB="$BUILD_ROOT/awsiot-terminal-agent_0.1.0_arm64.deb"
MANIFEST_PATH="$EDGE_DIR/awsiot-terminal-agent-rs/Cargo.toml"
EDGE_BINARY="$EDGE_DIR/awsiot-terminal-agent-rs/target/release/awsiot-terminal-agent-rs"

: "${AWSIOT_CLAIM_CERT_PEM_B64:?AWSIOT_CLAIM_CERT_PEM_B64 is required}"
: "${AWSIOT_CLAIM_KEY_PEM_B64:?AWSIOT_CLAIM_KEY_PEM_B64 is required}"

if [[ ! -d "$TEMPLATE_DIR" ]]; then
    echo "Template directory not found: $TEMPLATE_DIR" >&2
    exit 1
fi

rm -rf "$BUILD_ROOT"
mkdir -p "$BUILD_ROOT"
cp -r "$TEMPLATE_DIR" "$STAGE_DIR"

cargo build --release --manifest-path "$MANIFEST_PATH"

cp "$EDGE_BINARY" "$STAGE_DIR/opt/awsiot/bin/awsiot-terminal-agent"
cp "$EDGE_DIR/awsiot-device.conf" "$STAGE_DIR/opt/awsiot/conf/awsiot-device.conf"

printf '%s' "$AWSIOT_CLAIM_CERT_PEM_B64" | base64 -d > "$STAGE_DIR/opt/awsiot/certs/claim-certificate.pem.crt"
printf '%s' "$AWSIOT_CLAIM_KEY_PEM_B64" | base64 -d > "$STAGE_DIR/opt/awsiot/certs/claim-private.pem.key"

find "$STAGE_DIR" -type d -exec chmod 755 {} +
find "$STAGE_DIR" -type f -exec chmod 644 {} +
find "$STAGE_DIR/DEBIAN" -maxdepth 1 -type f -exec chmod 755 {} +
chmod 755 "$STAGE_DIR/opt/awsiot/bin/awsiot-terminal-agent"

dpkg-deb --build "$STAGE_DIR" "$OUTPUT_DEB"

echo "Built AWS IoT edge deb:"
echo "  $OUTPUT_DEB"
