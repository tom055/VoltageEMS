import argparse
import base64
import configparser
import json
import os
import re
import shutil
import sys
import threading
import time
import uuid
from pathlib import Path

import paho.mqtt.client as mqtt


DEFAULT_ENDPOINT = "d08715432na143627x533-ats.iot.us-east-1.amazonaws.com"
DEFAULT_PORT = 8883
DEFAULT_CA = r"C:\licheng\ssh\mqtt\AmazonRootCA1.pem"
DEFAULT_CERT = r"C:\licheng\ssh\mqtt\9a4589fdb6c16ee699def76f1c2512ff8a0cc9c9a386aec25daac6a54933256b-certificate.pem.crt"
DEFAULT_KEY = r"C:\licheng\ssh\mqtt\9a4589fdb6c16ee699def76f1c2512ff8a0cc9c9a386aec25daac6a54933256b-private.pem.key"
DEFAULT_SESSION_TIMEOUT_MINUTES = 10
DEFAULT_COMMAND_REPLY_TIMEOUT_SECONDS = 20

LOCAL_ESCAPE = b"\x1d"  # Ctrl+]
OSC_PATTERN = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")
DCS_PATTERN = re.compile(r"\x1bP.*?(?:\x1b\\)", re.DOTALL)
CSI_PATTERN = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
BRACKETED_PASTE_PATTERN = re.compile(r"\x1b\[\?2004[hl]")
INPUT_TERMINAL_RESPONSE_PATTERN = re.compile(r"^\x1b\[(?:\??|\>)[0-9;]*[cRn]$")
PROMPT_PATTERN = re.compile(r"(?<![A-Za-z0-9._-])(?P<prompt>[A-Za-z_][A-Za-z0-9._-]*@[A-Za-z0-9._-]+:[^\r\n#$]*[#$] )")


class OutputProcessor:
    def __init__(self) -> None:
        self.pending = b""
        self.alternate_screen = False
        self.last_prompt = ""

    def process(self, chunk: bytes) -> tuple[list[tuple[bytes, bool]], list[bytes], bool]:
        data = self.pending + chunk
        self.pending = b""
        plain = bytearray()
        raw = bytearray()
        segments: list[tuple[bytes, bool]] = []
        replies: list[bytes] = []
        prompt_arrived = False
        current_raw_mode = self.alternate_screen
        idx = 0

        def flush_plain() -> None:
            nonlocal prompt_arrived
            if not plain:
                return
            text = sanitize_output_text(bytes(plain).decode("utf-8", errors="replace"))
            text = normalize_plain_shell_output(text, self.last_prompt)
            matches = list(PROMPT_PATTERN.finditer(text))
            if matches:
                self.last_prompt = matches[-1].group("prompt")
                if matches[-1].end() == len(text):
                    text = text[: matches[-1].start()]
                    prompt_arrived = True
            segments.append((text.encode("utf-8", errors="replace"), False))
            plain.clear()

        def flush_raw() -> None:
            if not raw:
                return
            segments.append((bytes(raw), True))
            raw.clear()

        while idx < len(data):
            if current_raw_mode:
                if data[idx] != 0x1B:
                    raw.append(data[idx])
                    idx += 1
                    continue

                if idx + 1 >= len(data):
                    self.pending = data[idx:]
                    break

                marker = data[idx + 1]
                if marker == ord("]"):
                    end = self._find_osc_end(data, idx + 2)
                    if end is None:
                        self.pending = data[idx:]
                        break
                    idx = end
                    continue

                if marker == ord("P"):
                    end = self._find_osc_end(data, idx + 2)
                    if end is None:
                        self.pending = data[idx:]
                        break
                    idx = end
                    continue

                if marker == ord("["):
                    end = self._find_csi_end(data, idx + 2)
                    if end is None:
                        self.pending = data[idx:]
                        break
                    seq = data[idx : end + 1]
                    reply = self._build_reply(seq)
                    if reply is not None:
                        replies.append(reply)
                        idx = end + 1
                        continue
                    if BRACKETED_PASTE_PATTERN.fullmatch(seq.decode("ascii", errors="ignore")):
                        idx = end + 1
                        continue
                    self._update_terminal_state(seq)
                    raw.extend(seq)
                    idx = end + 1
                    if not self.alternate_screen:
                        flush_raw()
                        current_raw_mode = False
                    continue

                raw.append(data[idx])
                idx += 1
                continue

            if data[idx] != 0x1B:
                plain.append(data[idx])
                idx += 1
                continue

            if idx + 1 >= len(data):
                self.pending = data[idx:]
                break

            marker = data[idx + 1]
            if marker == ord("]"):
                end = self._find_osc_end(data, idx + 2)
                if end is None:
                    self.pending = data[idx:]
                    break
                idx = end
                continue

            if marker == ord("P"):
                end = self._find_osc_end(data, idx + 2)
                if end is None:
                    self.pending = data[idx:]
                    break
                idx = end
                continue

            if marker == ord("["):
                end = self._find_csi_end(data, idx + 2)
                if end is None:
                    self.pending = data[idx:]
                    break
                seq = data[idx : end + 1]
                reply = self._build_reply(seq)
                if BRACKETED_PASTE_PATTERN.fullmatch(seq.decode("ascii", errors="ignore")):
                    idx = end + 1
                    continue
                if reply is not None:
                    replies.append(reply)
                    idx = end + 1
                    continue
                self._update_terminal_state(seq)
                if self.alternate_screen:
                    flush_plain()
                    raw.extend(seq)
                    current_raw_mode = True
                else:
                    plain.extend(seq)
                idx = end + 1
                continue

            plain.append(data[idx])
            idx += 1

        if current_raw_mode:
            flush_raw()
        else:
            flush_plain()

        return segments, replies, prompt_arrived

    @staticmethod
    def _find_osc_end(data: bytes, start: int) -> int | None:
        idx = start
        while idx < len(data):
            if data[idx] == 0x07:
                return idx + 1
            if data[idx] == 0x1B and idx + 1 < len(data) and data[idx + 1] == ord("\\"):
                return idx + 2
            idx += 1
        return None

    @staticmethod
    def _find_csi_end(data: bytes, start: int) -> int | None:
        idx = start
        while idx < len(data):
            value = data[idx]
            if 0x40 <= value <= 0x7E:
                return idx
            idx += 1
        return None

    @staticmethod
    def _build_reply(seq: bytes) -> bytes | None:
        text = seq.decode("ascii", errors="ignore")
        if text in ("\x1b[c", "\x1b[0c"):
            return b"\x1b[?1;2c"
        if text in ("\x1b[>c", "\x1b[>0c"):
            return b"\x1b[>0;10;1c"
        if text == "\x1b[5n":
            return b"\x1b[0n"
        if text == "\x1b[6n":
            return b"\x1b[1;1R"
        return None

    def _update_terminal_state(self, seq: bytes) -> None:
        text = seq.decode("ascii", errors="ignore")
        if text in ("\x1b[?1049h", "\x1b[?47h", "\x1b[?1047h"):
            self.alternate_screen = True
        elif text in ("\x1b[?1049l", "\x1b[?47l", "\x1b[?1047l"):
            self.alternate_screen = False


def resolve_default_config_path() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().with_name("mqtt-terminal-cli.conf")
    return Path(__file__).resolve().with_name("mqtt-terminal-cli.conf")


def load_config(path: Path) -> dict[str, str]:
    config = configparser.ConfigParser()
    values = {
        "endpoint": DEFAULT_ENDPOINT,
        "port": str(DEFAULT_PORT),
        "ca": DEFAULT_CA,
        "cert": DEFAULT_CERT,
        "key": DEFAULT_KEY,
        "session_timeout_minutes": str(DEFAULT_SESSION_TIMEOUT_MINUTES),
        "command_reply_timeout_seconds": str(DEFAULT_COMMAND_REPLY_TIMEOUT_SECONDS),
    }
    if not path.exists():
        return values
    config.read(path, encoding="utf-8")
    if config.has_section("mqtt"):
        for key in ("endpoint", "port", "ca", "cert", "key"):
            if config.has_option("mqtt", key):
                values[key] = config.get("mqtt", key)
    if config.has_section("client"):
        for key in ("session_timeout_minutes", "command_reply_timeout_seconds"):
            if config.has_option("client", key):
                values[key] = config.get("client", key)
    return values


def build_topics(device_name: str) -> tuple[str, str]:
    return (
        f"device/{device_name}/terminal/in",
        f"device/{device_name}/terminal/out",
    )


def get_terminal_size() -> tuple[int, int]:
    size = shutil.get_terminal_size((160, 40))
    return max(10, size.lines), max(40, size.columns)


def enable_windows_vt_mode() -> None:
    if os.name != "nt":
        return
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        kernel32.SetConsoleCP(65001)
        kernel32.SetConsoleOutputCP(65001)
        handle = kernel32.GetStdHandle(-11)
        mode = ctypes.c_uint32()
        if kernel32.GetConsoleMode(handle, ctypes.byref(mode)):
            kernel32.SetConsoleMode(handle, mode.value | 0x0004)
    except Exception:
        pass


def sanitize_output_text(text: str) -> str:
    # Bash and Vim can emit OSC title updates that Windows consoles often show as raw text.
    text = OSC_PATTERN.sub("", text)
    text = DCS_PATTERN.sub("", text)
    text = BRACKETED_PASTE_PATTERN.sub("", text)
    return text


def raw_chunk_has_visible_text(data: bytes) -> bool:
    text = data.decode("utf-8", errors="replace")
    text = sanitize_output_text(text)
    text = CSI_PATTERN.sub("", text)
    return any(not ch.isspace() for ch in text)


def normalize_plain_shell_output(text: str, prompt_hint: str = "") -> str:
    if "\x1b" in text:
        return text
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"(?<!^)(?<!\n)(?=bash: )", "\n", text)
    if prompt_hint:
        text = text.replace(prompt_hint, "\n" + prompt_hint)
        if text.startswith("\n" + prompt_hint):
            text = text[1:]

    def ensure_prompt_newline(match: re.Match[str]) -> str:
        if match.start() == 0:
            return match.group("prompt")
        previous = text[match.start() - 1]
        if previous == "\n":
            return match.group("prompt")
        return "\n" + match.group("prompt")

    return PROMPT_PATTERN.sub(ensure_prompt_newline, text)


def write_output(data: bytes) -> None:
    if not data:
        return
    text = data.decode("utf-8", errors="replace")
    sys.stdout.write(text)
    sys.stdout.flush()


def write_raw_output(data: bytes) -> None:
    if not data:
        return
    text = data.decode("utf-8", errors="replace")
    sys.stdout.write(text)
    sys.stdout.flush()


class TerminalClient:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.topic_in, self.topic_out = build_topics(args.device)
        self.session_msg_id = uuid.uuid4().hex
        self.connected_event = threading.Event()
        self.connect_ack_event = threading.Event()
        self.disconnect_event = threading.Event()
        self.prompt_ready_event = threading.Event()
        self.stop_event = threading.Event()
        self.error_holder: dict[str, str | None] = {"value": None}
        self.connect_payload: dict | None = None
        self.disconnect_payload: dict | None = None
        self.output_lock = threading.Lock()
        self.last_size = get_terminal_size()
        self.session_deadline = time.monotonic() + max(1, args.session_timeout_minutes) * 60
        self.output_processor = OutputProcessor()
        self.drop_non_ascii_line = False
        self.shell_busy = False
        self.shell_output_received = False
        self.prompt_displayed = False
        self.pending_shell_command: str | None = None
        self.pending_shell_echo = False
        self.local_cursor_visible = True
        self.alt_insert_mode = False
        self.alt_screen_painted = False
        self.loading_visible = False
        self.loading_index = 0
        self.shell_wait_deadline = 0.0

        self.client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=args.client_id,
            clean_session=True,
        )
        self.client.tls_set(ca_certs=args.ca, certfile=args.cert, keyfile=args.key)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.will_set(
            self.topic_in,
            json.dumps(
                {
                    "state": "disconnect",
                    "msgId": self.session_msg_id,
                    "reason": "client_lost",
                }
            ),
            qos=1,
        )

    def on_connect(self, client: mqtt.Client, _userdata, _flags, reason_code, _properties=None) -> None:
        if str(reason_code) != "Success":
            self.error_holder["value"] = f"mqtt connect failed: {reason_code}"
            self.connected_event.set()
            return
        client.subscribe(self.topic_out, qos=1)
        self.connected_event.set()

    def on_message(self, _client: mqtt.Client, _userdata, msg: mqtt.MQTTMessage) -> None:
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except Exception:
            return

        state = str(payload.get("state", ""))
        msg_id = str(payload.get("msgId", ""))

        if state == "busy" and msg_id == self.session_msg_id:
            self.error_holder["value"] = f"session busy, activeMsgId={payload.get('activeMsgId', '')}"
            self.stop_event.set()
            self.connect_ack_event.set()
            return

        if msg_id != self.session_msg_id:
            return

        if state == "connect":
            self.connect_payload = payload
            self.connect_ack_event.set()
            return

        if state == "output":
            encoded = str(payload.get("data", ""))
            try:
                data = base64.b64decode(encoded)
            except Exception:
                data = b""
            segments, replies, prompt_arrived = self.output_processor.process(data)
            if self.pending_shell_echo and not self.in_alternate_screen():
                segments = self._strip_pending_shell_echo(segments)
            with self.output_lock:
                self.clear_loading_locked()
                for display_data, raw_mode in segments:
                    if raw_mode:
                        write_raw_output(display_data)
                        if self.in_alternate_screen() and raw_chunk_has_visible_text(display_data):
                            self.alt_screen_painted = True
                    else:
                        write_output(display_data)
            if not self.in_alternate_screen():
                self.shell_output_received = True
            for reply in replies:
                self.send_input(reply)
            if prompt_arrived and not self.in_alternate_screen():
                self.shell_busy = False
                self.prompt_ready_event.set()
            return

        if state == "disconnect":
            self.disconnect_payload = payload
            self.disconnect_event.set()
            self.stop_event.set()
            reason = str(payload.get("reason", "disconnect"))
            if reason not in {"client_exit", ""}:
                with self.output_lock:
                    sys.stdout.write(f"\n[session ended: {reason}]\n")
                    sys.stdout.flush()
            return

    def publish_json(self, payload: dict) -> None:
        self.client.publish(self.topic_in, json.dumps(payload), qos=1)

    def connect(self) -> None:
        self.client.connect(self.args.endpoint, self.args.port, keepalive=60)
        self.client.loop_start()

        while not self.connected_event.wait(timeout=0.2):
            pass
        if self.error_holder["value"]:
            raise RuntimeError(str(self.error_holder["value"]))

        rows, cols = self.last_size
        self.publish_json(
            {
                "state": "connect",
                "msgId": self.session_msg_id,
                "session_timeout_minutes": max(1, self.args.session_timeout_minutes),
                "rows": rows,
                "cols": cols,
            }
        )

        if not self.connect_ack_event.wait(timeout=max(1, self.args.command_reply_timeout_seconds)):
            raise RuntimeError("session connect handshake timed out")
        if self.error_holder["value"]:
            raise RuntimeError(str(self.error_holder["value"]))

        if self.connect_payload:
            initial_output = str(self.connect_payload.get("initial_output", ""))
            if initial_output:
                with self.output_lock:
                    sys.stdout.write(initial_output)
                    if not initial_output.endswith("\n"):
                        sys.stdout.write("\n")
                    sys.stdout.flush()

    def disconnect(self, reason: str) -> None:
        try:
            self.publish_json(
                {
                    "state": "disconnect",
                    "msgId": self.session_msg_id,
                    "reason": reason,
                }
            )
            self.disconnect_event.wait(timeout=max(1, self.args.command_reply_timeout_seconds))
        except Exception:
            pass

    def send_input(self, data: bytes) -> None:
        if not data:
            return
        self.publish_json(
            {
                "state": "input",
                "msgId": self.session_msg_id,
                "encoding": "base64",
                "data": base64.b64encode(data).decode("ascii"),
            }
        )

    def in_alternate_screen(self) -> bool:
        return self.output_processor.alternate_screen

    def set_local_cursor_visible(self, visible: bool) -> None:
        if self.local_cursor_visible == visible:
            return
        with self.output_lock:
            sys.stdout.write("\x1b[?25h" if visible else "\x1b[?25l")
            sys.stdout.flush()
        self.local_cursor_visible = visible

    def handle_non_ascii_shell_line(self) -> None:
        self.drop_non_ascii_line = True
        with self.output_lock:
            sys.stdout.write("\n[local warning: non-ASCII input is not sent in shell mode]\n")
            sys.stdout.flush()

    def restore_prompt_locally(self) -> None:
        prompt = self.output_processor.last_prompt or "# "
        with self.output_lock:
            sys.stdout.write(f"{prompt}")
            sys.stdout.flush()
        self.prompt_displayed = True

    def begin_shell_wait(self) -> None:
        self.shell_busy = True
        self.shell_output_received = False
        self.prompt_displayed = False
        self.prompt_ready_event.clear()
        self.loading_visible = False
        self.loading_index = 0
        self.shell_wait_deadline = time.monotonic() + max(2, self.args.command_reply_timeout_seconds)

    def clear_loading_locked(self) -> None:
        if not self.loading_visible:
            return
        sys.stdout.write("\r    \r")
        sys.stdout.flush()
        self.loading_visible = False

    def tick_loading(self) -> None:
        if (
            self.in_alternate_screen()
            or self.prompt_ready_event.is_set()
            or not self.shell_busy
            or self.shell_output_received
        ):
            return
        frames = [".  ", ".. ", "..."]
        with self.output_lock:
            sys.stdout.write("\r" + frames[self.loading_index % len(frames)])
            sys.stdout.flush()
            self.loading_visible = True
        self.loading_index += 1

    def render_prompt_if_ready(self) -> bool:
        if self.in_alternate_screen():
            return False
        if not self.prompt_ready_event.is_set():
            return False
        self.prompt_ready_event.clear()
        self.shell_busy = False
        self.set_local_cursor_visible(True)
        with self.output_lock:
            self.clear_loading_locked()
            sys.stdout.write(self.output_processor.last_prompt or "# ")
            sys.stdout.flush()
        self.prompt_displayed = True
        return True

    def shell_wait_timed_out(self) -> bool:
        return self.shell_busy and time.monotonic() >= self.shell_wait_deadline

    def _strip_pending_shell_echo(self, segments: list[tuple[bytes, bool]]) -> list[tuple[bytes, bool]]:
        command = self.pending_shell_command
        if not command:
            return segments
        normalized_command = command.replace("\r", "").replace("\n", "")
        stripped_segments: list[tuple[bytes, bool]] = []
        removed = False
        for data, raw_mode in segments:
            if removed or raw_mode:
                stripped_segments.append((data, raw_mode))
                continue
            text = data.decode("utf-8", errors="replace")
            leading_newlines = ""
            while text.startswith("\n") or text.startswith("\r\n"):
                if text.startswith("\r\n"):
                    leading_newlines += "\r\n"
                    text = text[2:]
                elif text.startswith("\n"):
                    leading_newlines += "\n"
                    text = text[1:]
            if text.startswith(normalized_command):
                text = text[len(normalized_command):]
                if text.startswith("\r\n"):
                    text = text[2:]
                elif text.startswith("\n"):
                    text = text[1:]
                removed = True
                text = leading_newlines + text
            if text:
                stripped_segments.append((text.encode("utf-8", errors="replace"), False))
            elif removed:
                continue
            else:
                stripped_segments.append((data, raw_mode))
        if removed:
            self.pending_shell_echo = False
        return stripped_segments

    def send_resize(self, rows: int, cols: int) -> None:
        self.publish_json(
            {
                "state": "resize",
                "msgId": self.session_msg_id,
                "rows": rows,
                "cols": cols,
            }
        )


def read_windows_key_chunk(raw_mode: bool = False) -> tuple[bool, bytes, bool]:
    import msvcrt

    if not msvcrt.kbhit():
        return False, b"", False

    if raw_mode:
        ch = msvcrt.getwch()
        if ch in ("\x00", "\xe0"):
            special = msvcrt.getwch()
            mapping = {
                "H": b"\x1b[A",
                "P": b"\x1b[B",
                "M": b"\x1b[C",
                "K": b"\x1b[D",
                "G": b"\x1b[H",
                "O": b"\x1b[F",
                "I": b"\x1b[5~",
                "Q": b"\x1b[6~",
                "S": b"\x1b[3~",
                "R": b"\x1b[2~",
            }
            return False, mapping.get(special, b""), False
        if ch == "\x1d":
            return True, b"", False
        if ch == "\r":
            return False, b"\n", False
        if ch == "\x08":
            return False, b"\x7f", False
        if ch == "\x03":
            return False, b"\x03", False
        saw_non_ascii = ord(ch) > 127
        payload = ch.encode("utf-8", errors="replace")
        if INPUT_TERMINAL_RESPONSE_PATTERN.fullmatch(payload.decode("ascii", errors="ignore")):
            return False, b"", False
        return False, payload, saw_non_ascii

    buffer = bytearray()
    saw_escape = False
    saw_non_ascii = False
    deadline = time.monotonic() + 0.01

    while True:
        if not msvcrt.kbhit():
            if saw_escape and time.monotonic() < deadline:
                time.sleep(0.002)
                continue
            break

        ch = msvcrt.getwch()
        if ch in ("\x00", "\xe0"):
            special = msvcrt.getwch()
            mapping = {
                "H": b"\x1b[A",
                "P": b"\x1b[B",
                "M": b"\x1b[C",
                "K": b"\x1b[D",
                "G": b"\x1b[H",
                "O": b"\x1b[F",
                "I": b"\x1b[5~",
                "Q": b"\x1b[6~",
                "S": b"\x1b[3~",
                "R": b"\x1b[2~",
            }
            buffer.extend(mapping.get(special, b""))
            continue

        if ch == "\x1d" and not buffer:
            return True, b"", False
        if ch == "\r":
            buffer.extend(b"\n")
            continue
        if ch == "\x08":
            buffer.extend(b"\x7f")
            continue
        if ch == "\x03":
            buffer.extend(b"\x03")
            continue

        if ord(ch) > 127:
            saw_non_ascii = True
        encoded = ch.encode("utf-8", errors="replace")
        buffer.extend(encoded)
        if ch == "\x1b":
            if raw_mode:
                break
            saw_escape = True
            deadline = time.monotonic() + 0.01

    payload = bytes(buffer)
    if INPUT_TERMINAL_RESPONSE_PATTERN.fullmatch(payload.decode("ascii", errors="ignore")):
        return False, b"", False
    return False, payload, saw_non_ascii


def filter_windows_payload(client: TerminalClient, payload: bytes, saw_non_ascii: bool) -> bytes:
    if not payload:
        return payload
    if client.in_alternate_screen():
        return payload

    if client.drop_non_ascii_line:
        if b"\n" in payload:
            client.drop_non_ascii_line = False
            return b""
        return b""

    if saw_non_ascii:
        client.handle_non_ascii_shell_line()
        client.restore_prompt_locally()
        if b"\n" in payload:
            client.drop_non_ascii_line = False
        return b""

    return payload


def erase_local_chars(count: int) -> None:
    if count <= 0:
        return
    sys.stdout.write("\b \b" * count)
    sys.stdout.flush()


def handle_shell_line_chunk(
    client: TerminalClient,
    payload: bytes,
    saw_non_ascii: bool,
    line_buffer: list[str],
    line_has_non_ascii: list[bool],
) -> None:
    if not payload:
        return

    if saw_non_ascii:
        line_has_non_ascii[0] = True
        return

    idx = 0
    while idx < len(payload):
        byte = payload[idx : idx + 1]
        idx += 1

        if byte == b"\x03":
            with client.output_lock:
                erase_local_chars(len("".join(line_buffer)))
                sys.stdout.write("^C\n")
                sys.stdout.write(client.output_processor.last_prompt or "# ")
                sys.stdout.flush()
            line_buffer.clear()
            line_has_non_ascii[0] = False
            continue

        if byte == b"\x7f":
            if line_buffer:
                removed = line_buffer.pop()
                with client.output_lock:
                    erase_local_chars(len(removed))
            continue

        if byte == b"\n":
            with client.output_lock:
                sys.stdout.write("\n")
                sys.stdout.flush()
            if line_has_non_ascii[0]:
                line_buffer.clear()
                line_has_non_ascii[0] = False
                client.handle_non_ascii_shell_line()
                client.restore_prompt_locally()
                continue

            command = "".join(line_buffer)
            line_buffer.clear()
            if not command.strip():
                client.restore_prompt_locally()
                continue

            client.send_input(command.encode("utf-8") + b"\n")
            client.begin_shell_wait()
            continue

        if byte == b"\x1b":
            continue

        try:
            ch = byte.decode("utf-8")
        except UnicodeDecodeError:
            continue
        line_buffer.append(ch)
        with client.output_lock:
            sys.stdout.write(ch)
            sys.stdout.flush()


def run_windows_terminal(client: TerminalClient) -> int:
    import msvcrt  # noqa: F401

    enable_windows_vt_mode()
    last_spinner_tick = 0.0

    startup_deadline = time.monotonic() + 0.8
    while (
        not client.stop_event.is_set()
        and not client.in_alternate_screen()
        and not client.prompt_ready_event.is_set()
        and time.monotonic() < startup_deadline
    ):
        time.sleep(0.02)

    if not client.prompt_ready_event.is_set() and not client.in_alternate_screen():
        client.send_input(b"\n")
        probe_deadline = time.monotonic() + 1.0
        while (
            not client.stop_event.is_set()
            and not client.in_alternate_screen()
            and not client.prompt_ready_event.is_set()
            and time.monotonic() < probe_deadline
        ):
            time.sleep(0.02)
    client.render_prompt_if_ready()

    while not client.stop_event.is_set():
        if time.monotonic() >= client.session_deadline:
            with client.output_lock:
                sys.stdout.write("\n[session expired]\n")
                sys.stdout.flush()
            break

        if client.in_alternate_screen():
            should_exit, payload, _ = read_windows_key_chunk(raw_mode=True)
            if should_exit:
                with client.output_lock:
                    sys.stdout.write("\n[local escape]\n")
                    sys.stdout.flush()
                return 0
            if payload:
                if (
                    not client.alt_insert_mode
                    and payload in {b"\x1b[A", b"\x1b[B", b"\x1b[C", b"\x1b[D", b"\x1b[H", b"\x1b[F", b"\x1b[5~", b"\x1b[6~"}
                ):
                    continue
                if payload in {b"i", b"I", b"a", b"A", b"o", b"O", b"c", b"C", b"s", b"S", b"R"}:
                    client.alt_insert_mode = True
                elif payload == b"\x1b":
                    client.alt_insert_mode = False
                client.send_input(payload)
            else:
                time.sleep(0.01)
            continue
        elif client.in_alternate_screen() is False:
            client.set_local_cursor_visible(True)
            client.alt_insert_mode = False
            client.alt_screen_painted = False

        if client.shell_busy:
            now = time.monotonic()
            if now - last_spinner_tick >= 0.25:
                client.tick_loading()
                last_spinner_tick = now
            if client.prompt_ready_event.wait(timeout=0.05):
                client.render_prompt_if_ready()
                continue
            if client.shell_wait_timed_out():
                client.shell_busy = False
                client.pending_shell_echo = False
                client.pending_shell_command = None
                with client.output_lock:
                    client.clear_loading_locked()
                    sys.stdout.write("\n[local warning: waiting for prompt timed out]\n")
                    sys.stdout.flush()
                continue
            time.sleep(0.01)
            continue

        prompt = client.output_processor.last_prompt or "# "
        if not client.prompt_displayed:
            with client.output_lock:
                sys.stdout.write(prompt)
                sys.stdout.flush()
            client.prompt_displayed = True
        try:
            command = sys.stdin.readline()
        except EOFError:
            break
        except KeyboardInterrupt:
            with client.output_lock:
                sys.stdout.write("\n")
                sys.stdout.flush()
            continue
        if command == "":
            break
        command = command.rstrip("\r\n")

        if not command.strip():
            client.prompt_displayed = False
            continue

        if any(ord(ch) > 127 for ch in command):
            client.prompt_displayed = False
            client.handle_non_ascii_shell_line()
            continue

        client.pending_shell_command = command
        client.pending_shell_echo = True
        client.send_input(command.encode("utf-8") + b"\n")
        client.begin_shell_wait()
    return 0


def run_posix_terminal(client: TerminalClient) -> int:
    import select
    import termios
    import tty

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    tty.setraw(fd)
    try:
        while not client.stop_event.is_set():
            if time.monotonic() >= client.session_deadline:
                with client.output_lock:
                    sys.stdout.write("\n[session expired]\n")
                    sys.stdout.flush()
                break
            ready, _, _ = select.select([fd], [], [], 0.05)
            if not ready:
                continue
            payload = os.read(fd, 1024)
            if not payload:
                break
            if payload == LOCAL_ESCAPE:
                with client.output_lock:
                    sys.stdout.write("\n[local escape]\n")
                    sys.stdout.flush()
                return 0
            payload = payload.replace(b"\r", b"\n")
            client.send_input(payload)
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
    return 0


def resize_watcher(client: TerminalClient) -> None:
    while not client.stop_event.wait(timeout=0.5):
        size = get_terminal_size()
        if size != client.last_size:
            client.last_size = size
            client.send_resize(*size)


def main() -> int:
    default_config_path = resolve_default_config_path()
    config_values = load_config(default_config_path)
    parser = argparse.ArgumentParser(
        description="Streaming MQTT terminal client for an edge device."
    )
    parser.add_argument("-d", "--device", required=True, help="target edge Thing name")
    parser.add_argument(
        "-l",
        "--session-timeout-minutes",
        type=int,
        default=int(config_values["session_timeout_minutes"]),
        help="session lifetime in minutes",
    )
    parser.add_argument(
        "-t",
        "--command-reply-timeout-seconds",
        type=int,
        default=int(config_values["command_reply_timeout_seconds"]),
        help="control-plane timeout in seconds for connect/disconnect operations",
    )
    parser.add_argument("--config", default=str(default_config_path), help="client config file path")
    parser.add_argument("--endpoint", default=config_values["endpoint"])
    parser.add_argument("--port", type=int, default=int(config_values["port"]))
    parser.add_argument("--ca", default=config_values["ca"])
    parser.add_argument("--cert", default=config_values["cert"])
    parser.add_argument("--key", default=config_values["key"])
    parser.add_argument("--client-id", default=f"mqtt-terminal-cli-{uuid.uuid4().hex[:8]}")
    args = parser.parse_args()

    client = TerminalClient(args)

    try:
        client.connect()
    except Exception as exc:
        print(f"connect error: {exc}", file=sys.stderr)
        return 1

    resize_thread = threading.Thread(target=resize_watcher, args=(client,), daemon=True)
    resize_thread.start()

    with client.output_lock:
        sys.stdout.write("[connected, local escape is Ctrl+]]\n")
        sys.stdout.flush()

    exit_code = 0
    try:
        if os.name == "nt":
            exit_code = run_windows_terminal(client)
        else:
            exit_code = run_posix_terminal(client)
    except KeyboardInterrupt:
        client.send_input(b"\x03")
    finally:
        if not client.disconnect_event.is_set():
            client.disconnect("client_exit")
        client.stop_event.set()
        client.client.loop_stop()
        client.client.disconnect()

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
