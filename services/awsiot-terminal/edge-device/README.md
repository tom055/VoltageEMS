# Edge Device

`edge-device` contains the edge-side Rust service, Fleet Provisioning assets, the Debian package staging directory, and the latest verified `arm64` package.

## Current Package

- `awsiot-terminal-agent_0.1.0_arm64.deb`

This is the package that was validated on `192.168.30.21`.

## Main Contents

- `awsiot-terminal-agent-rs`
  - Rust source code for the edge agent
- `awsiot-device.conf`
  - default runtime configuration template
- `fleet-provisioning`
  - claim certificate, claim private key, Root CA, and AWS-side reference files
- `awsiot-deb-arm64`
  - Debian package staging directory

## Functions

The edge service provides:

- Fleet Provisioning by claim
- automatic Thing registration on first boot
- automatic local runtime certificate generation and persistence
- runtime MQTT connection and topic subscription
- persistent PTY shell session per active client session
- reconnect after MQTT disconnect
- Thing existence verification before reconnect
- automatic re-registration if the Thing was deleted in AWS IoT

Current reconnect behavior:

- when MQTT is detected as disconnected, the service retries every `60` seconds
- before each retry it re-runs registration checks
- if the Thing no longer exists, it reprovisions and writes a new runtime certificate

## AWS Requirements

Before installation, AWS IoT must already have:

- region: `us-east-1`
- MQTT endpoint
- Fleet Provisioning template
- Thing Group
- claim certificate policy
- runtime device policy
- credential provider endpoint and role alias

The current default config expects:

- region: `us-east-1`
- endpoint: `d08715432na143627x533-ats.iot.us-east-1.amazonaws.com`
- Thing Group: `GreengrassQuickStartGroup`
- template: `GreengrassFleetProvisionTemplate`
- role alias: `AwsiotTerminalDeviceRoleAlias`

## Configuration

Installed config file:

- `/opt/awsiot/conf/awsiot-device.conf`

Important sections:

- `[aws]`
  - `region`
  - `mqtt_endpoint`
  - `thing_name_prefix`
  - `thing_group_name`
  - `device_sn_source`
- `[claim]`
  - `root_ca_path`
  - `cert_path`
  - `key_path`
- `[provisioning]`
  - `template_name`
  - `thing_type_name`
  - `connect_timeout_seconds`
  - `provision_timeout_seconds`
- `[mqtt]`
  - `port`
  - `ca_path`
  - `cert_path`
  - `key_path`
  - `connect_timeout_seconds`
- `[thing_sync]`
  - `check_aws_thing_existence`
  - `allow_local_fallback_on_check_error`
  - `credential_provider_endpoint`
  - `role_alias`
- `[thing_tags]`
  - static Thing metadata written into AWS IoT Thing attributes after successful provisioning
  - the service also re-syncs these attributes after each successful MQTT connect
  - missing keys from config are removed on sync
  - default config does not ship any user-defined attributes
  - the agent still adds a runtime-generated `create_time` attribute in local time with timezone, for example `2026-03-25 22:40:15 +08:00`
- `[agent]`
  - `session_state_path`
  - `shell_path`
  - `default_session_timeout_minutes`
  - `max_session_timeout_minutes`
  - `command_timeout_seconds`
  - `max_timeout_seconds`
  - `run_on_host`

## Installed Layout

After installation:

- binary: `/opt/awsiot/bin/awsiot-terminal-agent`
- config: `/opt/awsiot/conf/awsiot-device.conf`
- certs: `/opt/awsiot/certs`
- state: `/opt/awsiot/state`
- service: `/etc/systemd/system/awsiot-mqtt-terminal.service`

Files shipped in the package:

- `/opt/awsiot/certs/AmazonRootCA1.pem`
- `/opt/awsiot/certs/claim-certificate.pem.crt`
- `/opt/awsiot/certs/claim-private.pem.key`

Files created automatically after successful provisioning:

- `/opt/awsiot/certs/device.pem.crt`
- `/opt/awsiot/certs/device.private.pem.key`
- `/opt/awsiot/state/registration-state.json`
- `/opt/awsiot/state/session-state.json`

## Install

On the target edge device:

```bash
dpkg -i awsiot-terminal-agent_0.1.0_arm64.deb
```

Start the service:

```bash
systemctl start awsiot-mqtt-terminal.service
```

Check status:

```bash
systemctl status awsiot-mqtt-terminal.service --no-pager -l
```

## Enable Auto Start

Enable on boot:

```bash
systemctl enable awsiot-mqtt-terminal.service
```

Enable and start immediately:

```bash
systemctl enable --now awsiot-mqtt-terminal.service
```

Check whether startup is enabled:

```bash
systemctl is-enabled awsiot-mqtt-terminal.service
```

## Logs

The service writes logs to `journald`.

View recent logs:

```bash
journalctl -u awsiot-mqtt-terminal.service -n 100 --no-pager
```

Follow logs:

```bash
journalctl -u awsiot-mqtt-terminal.service -f
```

## Build And Package

Local Windows x86 build flow used in this repo:

```powershell
$env:Path = 'C:\tools\zig-0.15.2-clean\zig-x86_64-windows-0.15.2;C:\msys64\mingw64\bin;C:\msys64\ucrt64\bin;C:\msys64\usr\bin;' + $env:Path
$env:Path += ';' + "$HOME\.cargo\bin"
$env:RUSTUP_TOOLCHAIN = 'stable-x86_64-pc-windows-gnu'
cd C:\licheng\project\awsiot\edge-device\awsiot-terminal-agent-rs
cargo-zigbuild zigbuild --target aarch64-unknown-linux-gnu
```

Copy the new binary into the Debian staging directory:

```powershell
Copy-Item .\target\aarch64-unknown-linux-gnu\debug\awsiot-terminal-agent-rs ..\awsiot-deb-arm64\opt\awsiot\bin\awsiot-terminal-agent -Force
```

Build the `deb` on a Debian-like `arm64` host:

```bash
find awsiot-deb-arm64 -type d -exec chmod 755 {} +
find awsiot-deb-arm64 -type f -exec chmod 644 {} +
find awsiot-deb-arm64/DEBIAN -maxdepth 1 -type f -exec chmod 755 {} +
chmod 755 awsiot-deb-arm64/opt/awsiot/bin/awsiot-terminal-agent
dpkg-deb --build awsiot-deb-arm64 awsiot-terminal-agent_0.1.0_arm64.deb
```

## MQTT Topics

- edge subscribes:
  - `device/{thingName}/terminal/in`
- edge publishes:
  - `device/{thingName}/terminal/out`

Protocol details are in [MQTT-PROTOCOL.md](/C:/licheng/project/awsiot/client-cli/MQTT-PROTOCOL.md).
