# AWS IoT Terminal Agent

Installed files:

- `/opt/awsiot/bin/awsiot-terminal-agent`
- `/opt/awsiot/conf/awsiot-device.conf`
- `/opt/awsiot/certs`
- `/opt/awsiot/state/registration-state.json`
- `/opt/awsiot/state/session-state.json`

Behavior:

- The service uses Fleet Provisioning by claim
- It creates runtime device certificates on first registration
- It keeps one persistent PTY shell per active MQTT session
- It streams raw PTY output over MQTT
- It supports interactive terminal programs such as `vim`, `top`, `htop`, and `less`

Service:

- `systemctl enable awsiot-mqtt-terminal.service`
- `systemctl start awsiot-mqtt-terminal.service`
- `systemctl status awsiot-mqtt-terminal.service`

Logs:

- `journalctl -u awsiot-mqtt-terminal.service -f`
