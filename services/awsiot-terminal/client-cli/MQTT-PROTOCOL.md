# MQTT Streaming Terminal Protocol

## Overview

The client and edge device connect to the same AWS IoT Core broker and exchange streaming PTY data over MQTT.

Current environment:

- Region: `us-east-1`
- MQTT broker: `d08715432na143627x533-ats.iot.us-east-1.amazonaws.com`

This is a streaming terminal protocol:

- the edge keeps one PTY shell per active `msgId`
- shell state is preserved across commands
- the client sends raw terminal input bytes
- the edge returns raw PTY output bytes

## Topics

- client publishes:
  - `device/{thingName}/terminal/in`
- client subscribes:
  - `device/{thingName}/terminal/out`
- edge subscribes:
  - `device/{thingName}/terminal/in`
- edge publishes:
  - `device/{thingName}/terminal/out`

Example:

- `device/Greengrass-18b204606258a197/terminal/in`
- `device/Greengrass-18b204606258a197/terminal/out`

## Session Messages

### Connect request

```json
{
  "state": "connect",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "session_timeout_minutes": 10,
  "rows": 40,
  "cols": 120
}
```

### Connect response

```json
{
  "state": "connect",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "device_id": "Greengrass-18b204606258a197",
  "session_timeout_minutes": 10,
  "session_deadline_ts": 1774660000,
  "initial_output": "Welcome to EdgeLinux ...\n"
}
```

### Busy response

```json
{
  "state": "busy",
  "msgId": "new-msg-id",
  "device_id": "Greengrass-18b204606258a197",
  "activeMsgId": "current-active-msg-id"
}
```

### Disconnect request

```json
{
  "state": "disconnect",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "reason": "client_exit"
}
```

### Disconnect response

```json
{
  "state": "disconnect",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "device_id": "Greengrass-18b204606258a197",
  "reason": "client_exit"
}
```

Possible disconnect reasons:

- `client_exit`
- `client_lost`
- `shell_exit`
- `session_expired`

## Streaming PTY Messages

### Client input

```json
{
  "state": "input",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "encoding": "base64",
  "data": "Y2QgL3RtcA0="
}
```

`data` is base64-encoded terminal input bytes.

Examples:

- `pwd` + Enter
- `vim /tmp/file.txt` + Enter
- `:set number` inside `vim`
- arrow keys
- `Ctrl+C`
- `q` to exit `top`

### Edge output

```json
{
  "state": "output",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "device_id": "Greengrass-18b204606258a197",
  "encoding": "base64",
  "data": "G1sxOzQwci4uLg=="
}
```

`data` is base64-encoded PTY output bytes. It may contain:

- plain text
- ANSI control sequences
- full-screen redraws from `vim`, `top`, `less`, and similar apps

### Resize notification

```json
{
  "state": "resize",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "rows": 40,
  "cols": 120
}
```

The client sends this when the local terminal size changes.

## Timeout Behavior

### Session timeout

- configured by `-l`
- unit: minutes
- enforced by both client and edge

When the session expires:

- the edge closes the PTY session
- the edge publishes `disconnect` with `reason = session_expired`
- the client ends the local terminal session

### Control timeout

- configured by `-t`
- unit: seconds
- used for connect and disconnect control messages

This is no longer a per-command timeout because the session is now a continuous terminal stream.

## Last Will

The client sets MQTT Last Will on the input topic:

```json
{
  "state": "disconnect",
  "msgId": "7f4d31d66a2a48d5a53725c66f6230be",
  "reason": "client_lost"
}
```

If the client process crashes or the network drops, the edge clears the active PTY session.

## Local Escape

The Windows client uses:

- `Ctrl+]`

as the local escape to exit the client.

`Ctrl+C` is forwarded to the remote PTY.
