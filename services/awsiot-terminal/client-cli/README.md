# Client CLI

`client-cli` contains the local client used to connect to the edge MQTT terminal service.

## Main Contents

- `mqtt-terminal-cli.py`
  - canonical Python source
- `mqtt-terminal-cli.conf`
  - runtime configuration file
- `MQTT-PROTOCOL.md`
  - MQTT protocol reference

The packaged Windows executable is no longer stored in the repository.
It is built by the manual GitHub Actions workflow:

- `.github/workflows/awsiot-client-cli.yml`

## Configuration File

Client config file:

- `mqtt-terminal-cli.conf`

Current config format:

```ini
[mqtt]
endpoint = d08715432na143627x533-ats.iot.us-east-1.amazonaws.com
port = 8883
ca = ./ssl/AmazonRootCA1.pem
cert = ./ssl/client-certificate.pem.crt
key = ./ssl/client-private.pem.key

[client]
session_timeout_minutes = 10
command_reply_timeout_seconds = 20
```

Fields:

- `[mqtt].endpoint`
  - AWS IoT Core MQTT endpoint
- `[mqtt].port`
  - MQTT TLS port, normally `8883`
- `[mqtt].ca`
  - Root CA path
- `[mqtt].cert`
  - client certificate path
- `[mqtt].key`
  - client private key path
- `[client].session_timeout_minutes`
  - default session lifetime in minutes
- `[client].command_reply_timeout_seconds`
  - control-plane timeout in seconds for connect and disconnect

The config uses relative paths so the runtime files can live beside the executable:

- `./ssl/AmazonRootCA1.pem`
- `./ssl/client-certificate.pem.crt`
- `./ssl/client-private.pem.key`

## How To Use

Executable mode:

```powershell
.\mqtt-terminal-cli.v2.exe -d Greengrass-18b204606258a197 -l 10 -t 20
```

Python mode:

```powershell
python .\mqtt-terminal-cli.py -d Greengrass-18b204606258a197 -l 10 -t 20
```

Arguments:

- `-d`
  - target edge Thing name
- `-l`
  - session lifetime in minutes
- `-t`
  - connect/disconnect control timeout in seconds

Local escape:

- `Ctrl+]`

`Ctrl+C` is forwarded to the remote PTY.

## Requirements

When using `mqtt-terminal-cli.py`:

```powershell
python -m pip install --user paho-mqtt
```

When using the GitHub Actions packaged client:

- `AmazonRootCA1.pem` is bundled into `ssl/`
- if the repository secrets are configured, the packaged artifact also includes:
  - `ssl/client-certificate.pem.crt`
  - `ssl/client-private.pem.key`

Recommended GitHub Actions secrets:

- `MQTT_CLIENT_CERT_PEM_B64`
- `MQTT_CLIENT_KEY_PEM_B64`

Both values should be the Base64 of the complete PEM file content, including the `BEGIN` and `END` lines.

## Build

Use the manual GitHub Actions workflow:

- `.github/workflows/awsiot-client-cli.yml`

It builds `mqtt-terminal-cli.v2.exe` on `windows-latest` and uploads the artifact.

## MQTT Topics

- client publishes:
  - `device/{thingName}/terminal/in`
- client subscribes:
  - `device/{thingName}/terminal/out`

Protocol details are in [MQTT-PROTOCOL.md](C:/licheng/project/gitproject/VoltageEMS/services/awsiot-terminal/client-cli/MQTT-PROTOCOL.md).
