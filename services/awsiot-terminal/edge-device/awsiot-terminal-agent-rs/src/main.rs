use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_iot::Client as IotClient;
use aws_sdk_iot::error::ProvideErrorMetadata;
use aws_sdk_iot::types::AttributePayload;
use aws_types::region::Region;
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chrono::Local;
use clap::Parser;
use ini::Ini;
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use reqwest::{Certificate, Client as HttpClient, Identity};
use rumqttc::{AsyncClient, Event, EventLoop, Incoming, MqttOptions, Outgoing, QoS, TlsConfiguration, Transport};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::runtime::Handle;
use tokio::time::timeout;
use tracing::{info, warn};
use uuid::Uuid;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "/opt/awsiot/conf/awsiot-device.conf")]
    config: PathBuf,
    #[arg(long, default_value = "/opt/awsiot/state/registration-state.json")]
    registration_state: PathBuf,
    #[arg(long)]
    print_config: bool,
}

#[derive(Clone, Debug)]
struct AwsSection {
    region: String,
    mqtt_endpoint: String,
    thing_name_prefix: String,
    thing_group_name: String,
    device_sn_source: String,
}

#[derive(Clone, Debug)]
struct ClaimSection {
    root_ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
}

#[derive(Clone, Debug)]
struct ProvisioningSection {
    template_name: String,
    thing_type_name: String,
    connect_timeout_seconds: u64,
    provision_timeout_seconds: u64,
}

#[derive(Clone, Debug)]
struct MqttSection {
    port: u16,
    ca_path: PathBuf,
    cert_path: PathBuf,
    key_path: PathBuf,
    connect_timeout_seconds: u64,
}

#[derive(Clone, Debug)]
struct ThingSyncSection {
    check_aws_thing_existence: bool,
    allow_local_fallback_on_check_error: bool,
    credential_provider_endpoint: String,
    role_alias: String,
}

#[derive(Clone, Debug)]
struct AgentSection {
    session_state_path: PathBuf,
    shell_path: String,
    default_session_timeout_minutes: u64,
    max_session_timeout_minutes: u64,
    command_timeout_seconds: u64,
    max_timeout_seconds: u64,
    run_on_host: bool,
}

#[derive(Clone, Debug)]
struct ThingTagsSection {
    tags: HashMap<String, String>,
}

#[derive(Clone, Debug)]
struct AppConfig {
    aws: AwsSection,
    claim: ClaimSection,
    provisioning: ProvisioningSection,
    mqtt: MqttSection,
    thing_sync: ThingSyncSection,
    agent: AgentSection,
    thing_tags: ThingTagsSection,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct RegistrationState {
    registration_completed: bool,
    region: String,
    thing_name: String,
    thing_group_name: String,
    certificate_id: String,
    certificate_arn: String,
    device_sn: String,
    last_checked_at: String,
    last_result: String,
    mqtt_endpoint: String,
    device_cert_path: String,
    device_key_path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SessionState {
    state: String,
    #[serde(rename = "msgId")]
    msg_id: String,
    session_timeout_minutes: u64,
    session_deadline_ts: i64,
    disconnect_reason: String,
    updated_at: i64,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeView {
    mqtt_endpoint: String,
    mqtt_port: u16,
    mqtt_client_id: String,
    mqtt_topic_in: String,
    mqtt_topic_out: String,
    mqtt_ca_path: String,
    mqtt_cert_path: String,
    mqtt_key_path: String,
    session_state_path: String,
    shell_path: String,
    default_session_timeout_minutes: u64,
    max_session_timeout_minutes: u64,
    default_timeout_seconds: u64,
    max_timeout_seconds: u64,
    device_id: String,
    device_sn: String,
    run_on_host: bool,
    registration_state_path: String,
    config_path: String,
    mqtt_connect_timeout_seconds: u64,
    region: String,
}

#[derive(Clone, Debug)]
struct Runtime {
    mqtt_endpoint: String,
    mqtt_port: u16,
    mqtt_client_id: String,
    mqtt_topic_in: String,
    mqtt_topic_out: String,
    mqtt_ca_path: PathBuf,
    mqtt_cert_path: PathBuf,
    mqtt_key_path: PathBuf,
    session_state_path: PathBuf,
    shell_path: String,
    default_session_timeout_minutes: u64,
    max_session_timeout_minutes: u64,
    default_timeout_seconds: u64,
    max_timeout_seconds: u64,
    device_id: String,
    device_sn: String,
    run_on_host: bool,
    registration_state_path: PathBuf,
    config_path: PathBuf,
    mqtt_connect_timeout_seconds: u64,
    region: String,
}

#[derive(Clone, Debug, Deserialize)]
struct CredentialsResponse {
    credentials: TempCredentials,
}

#[derive(Clone, Debug, Deserialize)]
struct TempCredentials {
    #[serde(rename = "accessKeyId")]
    access_key_id: String,
    #[serde(rename = "secretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "sessionToken")]
    session_token: String,
}

#[derive(Clone, Debug, Deserialize)]
struct CreateCertificateResponse {
    #[serde(rename = "certificatePem")]
    certificate_pem: String,
    #[serde(rename = "privateKey")]
    private_key: String,
    #[serde(rename = "certificateId")]
    certificate_id: String,
    #[serde(rename = "certificateOwnershipToken")]
    certificate_ownership_token: String,
}

#[derive(Clone, Debug, Deserialize)]
struct RegisterThingResponse {
    #[serde(rename = "thingName")]
    thing_name: Option<String>,
}

#[derive(Clone, Debug)]
struct SessionStore {
    path: PathBuf,
    inner: Arc<Mutex<SessionState>>,
}

#[derive(Clone)]
struct ShellManager {
    inner: Arc<Mutex<Option<PersistentShell>>>,
}

#[derive(Clone)]
struct CommandContext {
    runtime: Runtime,
    session_store: SessionStore,
    shell_manager: ShellManager,
    mqtt: AsyncClient,
}

struct PersistentShell {
    msg_id: String,
    child: Box<dyn portable_pty::Child + Send>,
    master: Box<dyn portable_pty::MasterPty + Send>,
    writer: Box<dyn Write + Send>,
}

#[derive(Clone, Debug)]
struct ConnectAck {
    initial_output: String,
}

impl SessionStore {
    fn new(path: PathBuf) -> Result<Self> {
        let state = SessionState {
            state: "disconnect".to_string(),
            msg_id: String::new(),
            session_timeout_minutes: 0,
            session_deadline_ts: 0,
            disconnect_reason: "startup".to_string(),
            updated_at: current_timestamp(),
        };
        let store = Self {
            path,
            inner: Arc::new(Mutex::new(state)),
        };
        store.persist()?;
        Ok(store)
    }

    fn persist(&self) -> Result<()> {
        let state = self.snapshot();
        write_json_file(&self.path, &state)
    }

    fn connect(&self, msg_id: &str, session_timeout_minutes: u64) -> Result<SessionState> {
        let mut guard = self.inner.lock().map_err(|_| anyhow!("session state poisoned"))?;
        guard.state = "connect".to_string();
        guard.msg_id = msg_id.to_string();
        guard.session_timeout_minutes = session_timeout_minutes;
        guard.session_deadline_ts = current_timestamp() + (session_timeout_minutes as i64 * 60);
        guard.disconnect_reason.clear();
        guard.updated_at = current_timestamp();
        let snapshot = guard.clone();
        drop(guard);
        self.persist()?;
        Ok(snapshot)
    }

    fn disconnect(&self, reason: &str) -> Result<()> {
        let mut guard = self.inner.lock().map_err(|_| anyhow!("session state poisoned"))?;
        guard.state = "disconnect".to_string();
        guard.msg_id.clear();
        guard.session_timeout_minutes = 0;
        guard.session_deadline_ts = 0;
        guard.disconnect_reason = reason.to_string();
        guard.updated_at = current_timestamp();
        drop(guard);
        self.persist()
    }

    fn snapshot(&self) -> SessionState {
        self.inner
            .lock()
            .map(|state| state.clone())
            .unwrap_or(SessionState {
                state: "disconnect".to_string(),
                msg_id: String::new(),
                session_timeout_minutes: 0,
                session_deadline_ts: 0,
                disconnect_reason: "poisoned".to_string(),
                updated_at: current_timestamp(),
            })
    }
}

impl ShellManager {
    fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(None)),
        }
    }

    fn open_session(
        &self,
        runtime: &Runtime,
        msg_id: &str,
        rows: u16,
        cols: u16,
        mqtt: AsyncClient,
        topic_out: String,
        device_id: String,
        session_store: SessionStore,
        runtime_handle: Handle,
    ) -> Result<ConnectAck> {
        let mut guard = self.inner.lock().map_err(|_| anyhow!("shell manager poisoned"))?;
        if let Some(shell) = guard.as_ref()
            && shell.msg_id == msg_id
        {
            return Ok(ConnectAck {
                initial_output: String::new(),
            });
        }

        if let Some(mut existing) = guard.take() {
            existing.terminate();
        }

        let shell = PersistentShell::spawn(
            runtime,
            msg_id,
            rows,
            cols,
            mqtt,
            topic_out,
            device_id,
            session_store,
            runtime_handle,
        )?;
        *guard = Some(shell);
        Ok(ConnectAck {
            initial_output: build_login_banner(),
        })
    }

    fn send_input(&self, msg_id: &str, data: &[u8]) -> Result<()> {
        let mut guard = self.inner.lock().map_err(|_| anyhow!("shell manager poisoned"))?;
        let shell = guard
            .as_mut()
            .ok_or_else(|| anyhow!("shell session is not initialized"))?;
        if shell.msg_id != msg_id {
            bail!("shell session msgId does not match active session");
        }
        shell.send_input(data)
    }

    fn resize(&self, msg_id: &str, rows: u16, cols: u16) -> Result<()> {
        let mut guard = self.inner.lock().map_err(|_| anyhow!("shell manager poisoned"))?;
        let shell = guard
            .as_mut()
            .ok_or_else(|| anyhow!("shell session is not initialized"))?;
        if shell.msg_id != msg_id {
            bail!("shell session msgId does not match active session");
        }
        shell.resize(rows, cols)
    }

    fn close_session(&self) {
        if let Ok(mut guard) = self.inner.lock() {
            if let Some(mut shell) = guard.take() {
                shell.terminate();
            }
        }
    }

    fn close_session_if(&self, msg_id: &str) {
        if let Ok(mut guard) = self.inner.lock() {
            let should_close = guard
                .as_ref()
                .map(|shell| shell.msg_id == msg_id)
                .unwrap_or(false);
            if should_close
                && let Some(mut shell) = guard.take()
            {
                shell.terminate();
            }
        }
    }
}

impl PersistentShell {
    fn spawn(
        runtime: &Runtime,
        msg_id: &str,
        rows: u16,
        cols: u16,
        mqtt: AsyncClient,
        topic_out: String,
        device_id: String,
        session_store: SessionStore,
        runtime_handle: Handle,
    ) -> Result<Self> {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("open pty")?;
        let mut command = build_shell_command(runtime);
        command.env("TERM", "xterm-256color");
        command.env("PROMPT_COMMAND", "");
        command.env("PS1", "\\u@\\h:\\w\\$ ");
        if let Some(home_dir) = resolve_home_dir() {
            command.env("HOME", &home_dir);
            command.cwd(home_dir);
        }

        let child = pair
            .slave
            .spawn_command(command)
            .context("spawn persistent shell")?;
        let reader = pair.master.try_clone_reader().context("clone pty reader")?;
        let writer = pair.master.take_writer().context("take pty writer")?;

        spawn_output_forwarder(
            reader,
            mqtt,
            topic_out,
            device_id,
            msg_id.to_string(),
            session_store,
            runtime_handle,
        );

        Ok(Self {
            msg_id: msg_id.to_string(),
            child,
            master: pair.master,
            writer,
        })
    }

    fn send_input(&mut self, data: &[u8]) -> Result<()> {
        self.writer
            .write_all(data)
            .context("write input bytes to shell stdin")?;
        self.writer.flush().context("flush shell stdin")
    }

    fn resize(&mut self, rows: u16, cols: u16) -> Result<()> {
        self.master
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })
            .context("resize pty")
    }

    fn terminate(&mut self) {
        let _ = self.writer.write_all(b"exit\n");
        let _ = self.writer.flush();
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_output_forwarder<R>(
    mut reader: R,
    mqtt: AsyncClient,
    topic_out: String,
    device_id: String,
    msg_id: String,
    session_store: SessionStore,
    runtime_handle: Handle,
)
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffer = [0_u8; 4096];
        loop {
            match reader.read(&mut buffer) {
                Ok(0) => break,
                Ok(size) => {
                    let payload = json!({
                        "state": "output",
                        "msgId": msg_id,
                        "device_id": device_id,
                        "encoding": "base64",
                        "data": BASE64.encode(&buffer[..size]),
                    });
                    if publish_json_blocking(&runtime_handle, &mqtt, &topic_out, &payload).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }

        let snapshot = session_store.snapshot();
        if snapshot.state == "connect" && snapshot.msg_id == msg_id {
            let _ = session_store.disconnect("shell_exit");
            let _ = publish_json_blocking(
                &runtime_handle,
                &mqtt,
                &topic_out,
                &json!({
                    "state": "disconnect",
                    "msgId": msg_id,
                    "device_id": device_id,
                    "reason": "shell_exit",
                }),
            );
        }
    });
}

fn build_shell_command(runtime: &Runtime) -> CommandBuilder {
    if runtime.run_on_host {
        let mut command = CommandBuilder::new("nsenter");
        command.arg("--target");
        command.arg("1");
        command.arg("--mount");
        command.arg("--uts");
        command.arg("--ipc");
        command.arg("--net");
        command.arg("--pid");
        command.arg(&runtime.shell_path);
        if runtime.shell_path.ends_with("bash") {
            command.args(["--noprofile", "--norc", "-i"]);
        } else {
            command.arg("-i");
        }
        command
    } else {
        let mut command = CommandBuilder::new(&runtime.shell_path);
        if runtime.shell_path.ends_with("bash") {
            command.args(["--noprofile", "--norc", "-i"]);
        } else {
            command.arg("-i");
        }
        command
    }
}

fn resolve_home_dir() -> Option<PathBuf> {
    if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        return Some(PathBuf::from(home));
    }
    let root_home = PathBuf::from("/home/root");
    if root_home.exists() {
        return Some(root_home);
    }
    let fallback = PathBuf::from("/root");
    if fallback.exists() {
        return Some(fallback);
    }
    None
}

fn build_login_banner() -> String {
    let pretty_name = fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|content| {
            content.lines().find_map(|line| {
                line.strip_prefix("PRETTY_NAME=")
                    .map(|value| value.trim_matches('"').to_string())
            })
        })
        .unwrap_or_else(|| "Linux".to_string());
    let kernel = fs::read_to_string("/proc/sys/kernel/osrelease")
        .map(|value| value.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());
    let arch = std::env::consts::ARCH.to_string();

    let mut banner = format!("Welcome to {pretty_name} (GNU/Linux {kernel} {arch})\n\n");
    if Path::new("/var/run/reboot-required").exists() {
        banner.push_str("*** System restart required ***\n");
    }
    banner
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let args = Args::parse();

    if args.print_config {
        let runtime = build_runtime(&args.config, &args.registration_state).await?;
        let payload = RuntimeView {
            mqtt_endpoint: runtime.mqtt_endpoint.clone(),
            mqtt_port: runtime.mqtt_port,
            mqtt_client_id: runtime.mqtt_client_id.clone(),
            mqtt_topic_in: runtime.mqtt_topic_in.clone(),
            mqtt_topic_out: runtime.mqtt_topic_out.clone(),
            mqtt_ca_path: runtime.mqtt_ca_path.display().to_string(),
            mqtt_cert_path: runtime.mqtt_cert_path.display().to_string(),
            mqtt_key_path: runtime.mqtt_key_path.display().to_string(),
            session_state_path: runtime.session_state_path.display().to_string(),
            shell_path: runtime.shell_path.clone(),
            default_session_timeout_minutes: runtime.default_session_timeout_minutes,
            max_session_timeout_minutes: runtime.max_session_timeout_minutes,
            default_timeout_seconds: runtime.default_timeout_seconds,
            max_timeout_seconds: runtime.max_timeout_seconds,
            device_id: runtime.device_id.clone(),
            device_sn: runtime.device_sn.clone(),
            run_on_host: runtime.run_on_host,
            registration_state_path: runtime.registration_state_path.display().to_string(),
            config_path: runtime.config_path.display().to_string(),
            mqtt_connect_timeout_seconds: runtime.mqtt_connect_timeout_seconds,
            region: runtime.region.clone(),
        };
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    run_service_loop(args.config, args.registration_state).await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".parse().expect("valid default log level")),
        )
        .try_init();
}

enum RuntimeExit {
    Shutdown,
    Reconnect { reason: String },
}

async fn run_service_loop(config_path: PathBuf, registration_state_path: PathBuf) -> Result<()> {
    let reconnect_interval = Duration::from_secs(60);

    loop {
        let runtime = match build_runtime(&config_path, &registration_state_path).await {
            Ok(runtime) => runtime,
            Err(err) => {
                warn!(
                    "failed to prepare runtime; retrying in {} seconds: {err:#}",
                    reconnect_interval.as_secs()
                );
                tokio::time::sleep(reconnect_interval).await;
                continue;
            }
        };

        match run_runtime(runtime).await {
            Ok(RuntimeExit::Shutdown) => return Ok(()),
            Ok(RuntimeExit::Reconnect { reason }) => {
                warn!(
                    "runtime disconnected; retrying in {} seconds: {}",
                    reconnect_interval.as_secs(),
                    reason
                );
                tokio::time::sleep(reconnect_interval).await;
            }
            Err(err) => {
                warn!(
                    "runtime failed; retrying in {} seconds: {err:#}",
                    reconnect_interval.as_secs()
                );
                tokio::time::sleep(reconnect_interval).await;
            }
        }
    }
}

async fn run_runtime(runtime: Runtime) -> Result<RuntimeExit> {
    let session_store = SessionStore::new(runtime.session_state_path.clone())?;
    let shell_manager = ShellManager::new();
    let mut mqtt_options = build_mqtt_options(
        &runtime.mqtt_client_id,
        &runtime.mqtt_endpoint,
        runtime.mqtt_port,
        &runtime.mqtt_ca_path,
        &runtime.mqtt_cert_path,
        &runtime.mqtt_key_path,
    )?;
    mqtt_options.set_keep_alive(Duration::from_secs(60));

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 20);
    wait_for_connect(&mut eventloop, runtime.mqtt_connect_timeout_seconds).await?;
    if let Err(err) = sync_thing_attributes_on_connect(&runtime).await {
        warn!("failed to sync thing attributes after connect: {err:#}");
    }
    client
        .subscribe(runtime.mqtt_topic_in.clone(), QoS::AtLeastOnce)
        .await
        .context("subscribe runtime topic")?;

    let command_context = CommandContext {
        runtime: runtime.clone(),
        session_store: session_store.clone(),
        shell_manager: shell_manager.clone(),
        mqtt: client.clone(),
    };

    loop {
        tokio::select! {
            event = eventloop.poll() => {
                match event {
                    Ok(Event::Incoming(Incoming::Publish(packet))) => {
                        if packet.topic == runtime.mqtt_topic_in {
                            let ctx = command_context.clone();
                            tokio::spawn(async move {
                                if let Err(err) = handle_incoming_publish(ctx, packet.payload.to_vec()).await {
                                    warn!("failed to handle mqtt message: {err:#}");
                                }
                            });
                        }
                    }
                    Ok(Event::Incoming(Incoming::ConnAck(ack))) => {
                        info!("connected to {}:{} with reason_code={:?}", runtime.mqtt_endpoint, runtime.mqtt_port, ack.code);
                    }
                    Ok(Event::Outgoing(Outgoing::Disconnect)) => {
                        info!("disconnected");
                    }
                    Ok(_) => {}
                    Err(err) => {
                        shell_manager.close_session();
                        if let Err(disconnect_err) = session_store.disconnect("mqtt_disconnected") {
                            warn!("failed to mark session disconnected after mqtt error: {disconnect_err:#}");
                        }
                        return Ok(RuntimeExit::Reconnect {
                            reason: format!("runtime mqtt loop failed: {err}"),
                        });
                    }
                }
            }
            result = wait_for_shutdown_signal() => {
                result?;
                shell_manager.close_session();
                session_store.disconnect("service_shutdown")?;
                client.disconnect().await.context("disconnect runtime mqtt client")?;
                return Ok(RuntimeExit::Shutdown);
            }
        }
    }
}

async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .context("register SIGTERM handler")?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = terminate.recv() => {}
        }
        Ok(())
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context("register Ctrl+C handler")?;
        Ok(())
    }
}

async fn handle_incoming_publish(ctx: CommandContext, payload: Vec<u8>) -> Result<()> {
    let message: Value = serde_json::from_slice(&payload).context("parse mqtt json payload")?;
    let state = message.get("state").and_then(Value::as_str).unwrap_or_default().to_string();
    let msg_id = message.get("msgId").and_then(Value::as_str).unwrap_or_default().to_string();
    info!("incoming mqtt payload state='{}' msgId='{}' body={}", state, msg_id, message);

    if state == "connect" {
        let snapshot = ctx.session_store.snapshot();
        if snapshot.state == "connect" && !snapshot.msg_id.is_empty() && snapshot.msg_id != msg_id {
            info!("session busy current_msg_id='{}' requested_msg_id='{}'", snapshot.msg_id, msg_id);
            publish_json(
                &ctx.mqtt,
                &ctx.runtime.mqtt_topic_out,
                &json!({
                    "state": "busy",
                    "msgId": msg_id,
                    "device_id": ctx.runtime.device_id,
                    "activeMsgId": snapshot.msg_id,
                }),
            )
            .await?;
            return Ok(());
        }

        let session_timeout_minutes = message
            .get("session_timeout_minutes")
            .and_then(Value::as_u64)
            .unwrap_or(ctx.runtime.default_session_timeout_minutes)
            .clamp(1, ctx.runtime.max_session_timeout_minutes);
        let rows = message
            .get("rows")
            .and_then(Value::as_u64)
            .unwrap_or(40)
            .clamp(10, 200) as u16;
        let cols = message
            .get("cols")
            .and_then(Value::as_u64)
            .unwrap_or(160)
            .clamp(40, 400) as u16;
        let connected = ctx.session_store.connect(&msg_id, session_timeout_minutes)?;
        let runtime = ctx.runtime.clone();
        let shell_manager = ctx.shell_manager.clone();
        let connect_msg_id = msg_id.clone();
        let mqtt = ctx.mqtt.clone();
        let topic_out = ctx.runtime.mqtt_topic_out.clone();
        let device_id = ctx.runtime.device_id.clone();
        let session_store = ctx.session_store.clone();
        let runtime_handle = Handle::current();
        let connect_ack = tokio::task::spawn_blocking(move || {
            shell_manager.open_session(
                &runtime,
                &connect_msg_id,
                rows,
                cols,
                mqtt,
                topic_out,
                device_id,
                session_store,
                runtime_handle,
            )
        })
        .await
        .context("join shell connect task")??;
        info!(
            "session connected msg_id='{}' session_timeout_minutes={} session_deadline_ts={}",
            msg_id,
            connected.session_timeout_minutes,
            connected.session_deadline_ts
        );
        publish_json(
            &ctx.mqtt,
            &ctx.runtime.mqtt_topic_out,
            &json!({
                "state": "connect",
                "msgId": msg_id,
                "device_id": ctx.runtime.device_id,
                "session_timeout_minutes": connected.session_timeout_minutes,
                "session_deadline_ts": connected.session_deadline_ts,
                "initial_output": connect_ack.initial_output,
            }),
        )
        .await?;
        spawn_session_timeout_task(ctx.clone(), msg_id.clone(), connected.session_deadline_ts);
        return Ok(());
    }

    if state == "disconnect" {
        let snapshot = ctx.session_store.snapshot();
        if snapshot.state == "connect" && !snapshot.msg_id.is_empty() && snapshot.msg_id != msg_id {
            warn!(
                "ignore disconnect for mismatched msgId current='{}' incoming='{}'",
                snapshot.msg_id,
                msg_id
            );
            return Ok(());
        }
        let reason = message
            .get("reason")
            .and_then(Value::as_str)
            .unwrap_or("client_disconnect");
        ctx.session_store.disconnect(reason)?;
        let shell_manager = ctx.shell_manager.clone();
        let disconnect_msg_id = msg_id.clone();
        tokio::task::spawn_blocking(move || shell_manager.close_session_if(&disconnect_msg_id))
            .await
            .context("join shell disconnect task")?;
        info!("session disconnected msg_id='{}' reason='{}'", msg_id, reason);
        publish_json(
            &ctx.mqtt,
            &ctx.runtime.mqtt_topic_out,
            &json!({
                "state": "disconnect",
                "msgId": msg_id,
                "device_id": ctx.runtime.device_id,
                "reason": reason,
            }),
        )
        .await?;
        return Ok(());
    }

    let snapshot = ctx.session_store.snapshot();
    if snapshot.state == "connect" && snapshot.session_deadline_ts > 0 && current_timestamp() > snapshot.session_deadline_ts {
        warn!(
            "input rejected due to expired session session_msg_id='{}' request_msg_id='{}' deadline_ts={}",
            snapshot.msg_id,
            msg_id,
            snapshot.session_deadline_ts
        );
        ctx.session_store.disconnect("session_expired")?;
        let shell_manager = ctx.shell_manager.clone();
        let expired_msg_id = snapshot.msg_id.clone();
        tokio::task::spawn_blocking(move || shell_manager.close_session_if(&expired_msg_id))
            .await
            .context("join shell expiry task")?;
        publish_json(
            &ctx.mqtt,
            &ctx.runtime.mqtt_topic_out,
            &json!({
                "state": "disconnect",
                "msgId": msg_id,
                "device_id": ctx.runtime.device_id,
                "reason": "session_expired",
            }),
        )
        .await?;
        return Ok(());
    }

    if snapshot.state != "connect" || snapshot.msg_id != msg_id {
        warn!(
            "input rejected session_state='{}' session_msg_id='{}' request_msg_id='{}'",
            snapshot.state,
            snapshot.msg_id,
            msg_id
        );
        return Ok(());
    }

    if state == "resize" {
        let rows = message
            .get("rows")
            .and_then(Value::as_u64)
            .unwrap_or(40)
            .clamp(10, 200) as u16;
        let cols = message
            .get("cols")
            .and_then(Value::as_u64)
            .unwrap_or(160)
            .clamp(40, 400) as u16;
        let shell_manager = ctx.shell_manager.clone();
        let resize_msg_id = msg_id.clone();
        tokio::task::spawn_blocking(move || shell_manager.resize(&resize_msg_id, rows, cols))
            .await
            .context("join shell resize task")??;
        return Ok(());
    }

    if state == "input" {
        let encoded = message
            .get("data")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if encoded.is_empty() {
            return Ok(());
        }
        let input = BASE64
            .decode(encoded)
            .context("decode input bytes from base64")?;
        let shell_manager = ctx.shell_manager.clone();
        let input_msg_id = msg_id.clone();
        tokio::task::spawn_blocking(move || shell_manager.send_input(&input_msg_id, &input))
            .await
            .context("join shell input task")??;
        return Ok(());
    }

    warn!("ignored unsupported terminal state='{}' msgId='{}'", state, msg_id);
    Ok(())
}

async fn publish_json(client: &AsyncClient, topic: &str, payload: &Value) -> Result<()> {
    info!("publishing topic='{}' payload={}", topic, payload);
    client
        .publish(topic, QoS::AtLeastOnce, false, serde_json::to_vec(payload)?)
        .await
        .with_context(|| format!("publish to {topic}"))?;
    Ok(())
}

fn publish_json_blocking(client_runtime: &Handle, client: &AsyncClient, topic: &str, payload: &Value) -> Result<()> {
    let bytes = serde_json::to_vec(payload)?;
    let topic_owned = topic.to_string();
    client_runtime.block_on(async move {
        client
            .publish(topic_owned, QoS::AtLeastOnce, false, bytes)
            .await
            .context("publish json from output forwarder")
    })
}

fn spawn_session_timeout_task(ctx: CommandContext, msg_id: String, deadline_ts: i64) {
    tokio::spawn(async move {
        let wait_seconds = (deadline_ts - current_timestamp()).max(0) as u64;
        tokio::time::sleep(Duration::from_secs(wait_seconds)).await;
        let snapshot = ctx.session_store.snapshot();
        if snapshot.state != "connect" || snapshot.msg_id != msg_id {
            return;
        }
        if snapshot.session_deadline_ts > current_timestamp() {
            return;
        }

        let shell_manager = ctx.shell_manager.clone();
        let expired_msg_id = msg_id.clone();
        let _ = ctx.session_store.disconnect("session_expired");
        let _ = tokio::task::spawn_blocking(move || shell_manager.close_session_if(&expired_msg_id)).await;
        let _ = publish_json(
            &ctx.mqtt,
            &ctx.runtime.mqtt_topic_out,
            &json!({
                "state": "disconnect",
                "msgId": msg_id,
                "device_id": ctx.runtime.device_id,
                "reason": "session_expired",
            }),
        )
        .await;
    });
}

async fn build_runtime(config_path: &Path, registration_state_path: &Path) -> Result<Runtime> {
    let config = load_config(config_path)?;
    let registration_state = ensure_registration(&config, registration_state_path).await?;
    let thing_name = registration_state.thing_name.clone();

    Ok(Runtime {
        mqtt_endpoint: config.aws.mqtt_endpoint.clone(),
        mqtt_port: config.mqtt.port,
        mqtt_client_id: thing_name.clone(),
        mqtt_topic_in: format!("device/{thing_name}/terminal/in"),
        mqtt_topic_out: format!("device/{thing_name}/terminal/out"),
        mqtt_ca_path: config.mqtt.ca_path.clone(),
        mqtt_cert_path: config.mqtt.cert_path.clone(),
        mqtt_key_path: config.mqtt.key_path.clone(),
        session_state_path: config.agent.session_state_path.clone(),
        shell_path: config.agent.shell_path.clone(),
        default_session_timeout_minutes: config.agent.default_session_timeout_minutes,
        max_session_timeout_minutes: config.agent.max_session_timeout_minutes,
        default_timeout_seconds: config.agent.command_timeout_seconds,
        max_timeout_seconds: config.agent.max_timeout_seconds,
        device_id: thing_name,
        device_sn: registration_state.device_sn,
        run_on_host: config.agent.run_on_host,
        registration_state_path: registration_state_path.to_path_buf(),
        config_path: config_path.to_path_buf(),
        mqtt_connect_timeout_seconds: config.mqtt.connect_timeout_seconds,
        region: config.aws.region.clone(),
    })
}

async fn ensure_registration(config: &AppConfig, registration_state_path: &Path) -> Result<RegistrationState> {
    let device_sn = read_device_sn(&config.aws.device_sn_source)?;
    let expected_thing_name = format!("{}{}", config.aws.thing_name_prefix, device_sn);
    let cert_path = config.mqtt.cert_path.clone();
    let key_path = config.mqtt.key_path.clone();
    let registration_state = load_registration_state(registration_state_path)?;

    if registration_state.registration_completed
        && registration_state.thing_name == expected_thing_name
        && registration_state.device_sn == device_sn
        && cert_path.exists()
        && key_path.exists()
    {
        if config.thing_sync.check_aws_thing_existence {
            match thing_exists_in_aws(config, &cert_path, &key_path, &expected_thing_name).await {
                Ok(true) => {
                    let mut updated = registration_state.clone();
                    updated.last_checked_at = now_string();
                    updated.last_result = "verified_existing_thing".to_string();
                    write_json_file(registration_state_path, &updated)?;
                    return Ok(updated);
                }
                Ok(false) => {
                    let _ = fs::remove_file(&cert_path);
                    let _ = fs::remove_file(&key_path);
                    let mut updated = registration_state.clone();
                    updated.last_checked_at = now_string();
                    updated.last_result = "aws_thing_missing_reprovisioning".to_string();
                    updated.registration_completed = false;
                    write_json_file(registration_state_path, &updated)?;
                    return provision_device(config, registration_state_path).await;
                }
                Err(err) if config.thing_sync.allow_local_fallback_on_check_error => {
                    warn!("aws thing existence check failed, falling back to local state: {err:#}");
                    return Ok(registration_state);
                }
                Err(err) => return Err(err),
            }
        }

        return Ok(registration_state);
    }

    provision_device(config, registration_state_path).await
}

async fn thing_exists_in_aws(
    config: &AppConfig,
    runtime_cert_path: &Path,
    runtime_key_path: &Path,
    thing_name: &str,
) -> Result<bool> {
    let mut errors = Vec::new();

    if runtime_cert_path.exists() && runtime_key_path.exists() {
        match thing_exists_with_credentials(config, runtime_cert_path, runtime_key_path, thing_name).await {
            Ok(value) => return Ok(value),
            Err(err) => errors.push(err.to_string()),
        }
    }

        match thing_exists_with_credentials(
            config,
            &config.claim.cert_path,
            &config.claim.key_path,
            thing_name,
        )
    .await
    {
        Ok(value) => Ok(value),
        Err(err) => {
            errors.push(err.to_string());
            bail!("{}", errors.join("; "));
        }
    }
}

async fn thing_exists_with_credentials(
    config: &AppConfig,
    cert_path: &Path,
    key_path: &Path,
    thing_name: &str,
) -> Result<bool> {
    let sdk_config = iot_sdk_config_from_certificate(config, cert_path, key_path).await?;
    let client = IotClient::new(&sdk_config);
    let response = client.describe_thing().thing_name(thing_name).send().await;
    match response {
        Ok(_) => Ok(true),
        Err(err) => {
            if let Some(service_err) = err.as_service_error() {
                if service_err.code() == Some("ResourceNotFoundException") {
                    return Ok(false);
                }
            }
            Err(anyhow!("describe thing failed: {err}"))
        }
    }
}

async fn sync_thing_attributes_on_connect(runtime: &Runtime) -> Result<()> {
    let config = load_config(&runtime.config_path)?;
    let sdk_config =
        iot_sdk_config_from_certificate(&config, &runtime.mqtt_cert_path, &runtime.mqtt_key_path).await?;
    let client = IotClient::new(&sdk_config);
    let response = client
        .describe_thing()
        .thing_name(&runtime.device_id)
        .send()
        .await
        .context("describe thing for attribute sync")?;

    let current_attributes = response.attributes().cloned().unwrap_or_default();
    let mut desired_attributes: HashMap<String, String> = HashMap::new();

    if let Some(serial_number) = current_attributes.get("serialNumber") {
        desired_attributes.insert("serialNumber".to_string(), serial_number.clone());
    }
    if let Some(create_time) = current_attributes.get("create_time") {
        desired_attributes.insert("create_time".to_string(), create_time.clone());
    }
    for (key, value) in &config.thing_tags.tags {
        desired_attributes.insert(key.clone(), value.clone());
    }

    if desired_attributes == current_attributes {
        return Ok(());
    }

    let attribute_payload = AttributePayload::builder()
        .set_attributes(Some(desired_attributes))
        .merge(false)
        .build();

    client
        .update_thing()
        .thing_name(&runtime.device_id)
        .expected_version(response.version())
        .attribute_payload(attribute_payload)
        .send()
        .await
        .context("update thing attributes on connect")?;

    Ok(())
}

async fn iot_sdk_config_from_certificate(
    config: &AppConfig,
    cert_path: &Path,
    key_path: &Path,
) -> Result<aws_types::SdkConfig> {
    let credentials = fetch_iot_credentials(config, cert_path, key_path).await?;
    let provider = aws_credential_types::provider::SharedCredentialsProvider::new(Credentials::new(
        credentials.access_key_id,
        credentials.secret_access_key,
        Some(credentials.session_token),
        None,
        "iot-credential-provider",
    ));

    Ok(aws_config::defaults(BehaviorVersion::latest())
        .region(Region::new(config.aws.region.clone()))
        .credentials_provider(provider)
        .load()
        .await)
}

async fn fetch_iot_credentials(config: &AppConfig, cert_path: &Path, key_path: &Path) -> Result<TempCredentials> {
    let cert_bytes = fs::read(cert_path).with_context(|| format!("read cert {}", cert_path.display()))?;
    let key_bytes = fs::read(key_path).with_context(|| format!("read key {}", key_path.display()))?;
    let mut identity_pem = cert_bytes;
    identity_pem.push(b'\n');
    identity_pem.extend(key_bytes);
    let identity = Identity::from_pem(&identity_pem).context("build mTLS identity")?;
    let ca = Certificate::from_pem(
        &fs::read(&config.claim.root_ca_path)
            .with_context(|| format!("read root ca {}", config.claim.root_ca_path.display()))?,
    )
    .context("parse root CA")?;
    let client = HttpClient::builder()
        .use_rustls_tls()
        .identity(identity)
        .add_root_certificate(ca)
        .connect_timeout(Duration::from_secs(15))
        .timeout(Duration::from_secs(30))
        .build()
        .context("build credential provider client")?;

    let url = format!(
        "https://{}/role-aliases/{}/credentials",
        config.thing_sync.credential_provider_endpoint, config.thing_sync.role_alias
    );
    let response = client
        .get(url)
        .send()
        .await
        .context("call credential provider")?
        .error_for_status()
        .context("credential provider returned error")?;

    Ok(response
        .json::<CredentialsResponse>()
        .await
        .context("parse credential provider response")?
        .credentials)
}

async fn provision_device(config: &AppConfig, registration_state_path: &Path) -> Result<RegistrationState> {
    let device_sn = read_device_sn(&config.aws.device_sn_source)?;
    let thing_name = format!("{}{}", config.aws.thing_name_prefix, device_sn);
    let client_id = format!("fleet-claim-{}", &Uuid::new_v4().simple().to_string()[..12]);

    let mut mqtt_options = build_mqtt_options(
        &client_id,
        &config.aws.mqtt_endpoint,
        config.mqtt.port,
        &config.claim.root_ca_path,
        &config.claim.cert_path,
        &config.claim.key_path,
    )?;
    mqtt_options.set_keep_alive(Duration::from_secs(60));

    let (client, mut eventloop) = AsyncClient::new(mqtt_options, 20);
    wait_for_connect(&mut eventloop, config.provisioning.connect_timeout_seconds).await?;

    let create_accepted = "$aws/certificates/create/json/accepted";
    let create_rejected = "$aws/certificates/create/json/rejected";
    client.subscribe(create_accepted, QoS::AtLeastOnce).await?;
    client.subscribe(create_rejected, QoS::AtLeastOnce).await?;
    client
        .publish("$aws/certificates/create/json", QoS::AtLeastOnce, false, "{}")
        .await
        .context("request create certificate")?;

    let create_response: CreateCertificateResponse = wait_for_json_response(
        &mut eventloop,
        create_accepted,
        create_rejected,
        config.provisioning.provision_timeout_seconds,
    )
    .await?;

    let register_accepted = format!(
        "$aws/provisioning-templates/{}/provision/json/accepted",
        config.provisioning.template_name
    );
    let register_rejected = format!(
        "$aws/provisioning-templates/{}/provision/json/rejected",
        config.provisioning.template_name
    );
    client.subscribe(register_accepted.clone(), QoS::AtLeastOnce).await?;
    client.subscribe(register_rejected.clone(), QoS::AtLeastOnce).await?;
    let create_time = thing_create_time_string();
    let register_payload = json!({
        "certificateOwnershipToken": create_response.certificate_ownership_token,
        "parameters": {
            "ThingName": thing_name,
            "SerialNumber": device_sn,
            "ThingTypeName": config.provisioning.thing_type_name,
            "CreateTime": create_time,
        }
    });
    client
        .publish(
            format!(
                "$aws/provisioning-templates/{}/provision/json",
                config.provisioning.template_name
            ),
            QoS::AtLeastOnce,
            false,
            serde_json::to_vec(&register_payload)?,
        )
        .await
        .context("request register thing")?;

    let register_response: RegisterThingResponse = wait_for_json_response(
        &mut eventloop,
        &register_accepted,
        &register_rejected,
        config.provisioning.provision_timeout_seconds,
    )
    .await?;

    let _ = client.disconnect().await;
    persist_device_credentials(config, &create_response.certificate_pem, &create_response.private_key)?;

    let final_thing_name = register_response.thing_name.unwrap_or_else(|| thing_name.clone());
    let state = RegistrationState {
        registration_completed: true,
        region: config.aws.region.clone(),
        thing_name: final_thing_name,
        thing_group_name: config.aws.thing_group_name.clone(),
        certificate_id: create_response.certificate_id.clone(),
        certificate_arn: String::new(),
        device_sn,
        last_checked_at: now_string(),
        last_result: "fleet_provisioned".to_string(),
        mqtt_endpoint: config.aws.mqtt_endpoint.clone(),
        device_cert_path: config.mqtt.cert_path.display().to_string(),
        device_key_path: config.mqtt.key_path.display().to_string(),
    };
    write_json_file(registration_state_path, &state)?;
    Ok(state)
}

async fn wait_for_json_response<T: for<'de> Deserialize<'de>>(
    eventloop: &mut EventLoop,
    accepted_topic: &str,
    rejected_topic: &str,
    timeout_seconds: u64,
) -> Result<T> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("mqtt response timed out after {timeout_seconds} seconds");
        }

        let event = timeout(remaining, eventloop.poll())
            .await
            .context("mqtt response wait timed out")?
            .context("poll mqtt response")?;
        match event {
            Event::Incoming(Incoming::Publish(packet)) if packet.topic == accepted_topic => {
                return serde_json::from_slice::<T>(&packet.payload).context("parse accepted response");
            }
            Event::Incoming(Incoming::Publish(packet)) if packet.topic == rejected_topic => {
                let body = String::from_utf8_lossy(packet.payload.as_ref()).to_string();
                bail!("mqtt provisioning request rejected: {body}");
            }
            _ => {}
        }
    }
}

async fn wait_for_connect(eventloop: &mut EventLoop, timeout_seconds: u64) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(timeout_seconds);
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("mqtt connect timed out after {timeout_seconds} seconds");
        }

        let event = timeout(remaining, eventloop.poll())
            .await
            .context("wait for connect timed out")?
            .context("poll mqtt connect")?;
        if let Event::Incoming(Incoming::ConnAck(ack)) = event {
            info!("connected with reason_code={:?}", ack.code);
            return Ok(());
        }
    }
}

fn build_mqtt_options(
    client_id: &str,
    endpoint: &str,
    port: u16,
    ca_path: &Path,
    cert_path: &Path,
    key_path: &Path,
) -> Result<MqttOptions> {
    let mut options = MqttOptions::new(client_id, endpoint, port);
    let ca = fs::read(ca_path).with_context(|| format!("read CA {}", ca_path.display()))?;
    let cert = fs::read(cert_path).with_context(|| format!("read cert {}", cert_path.display()))?;
    let key = fs::read(key_path).with_context(|| format!("read key {}", key_path.display()))?;
    options.set_transport(Transport::Tls(TlsConfiguration::Simple {
        ca,
        alpn: None,
        client_auth: Some((cert, key)),
    }));
    Ok(options)
}

fn persist_device_credentials(config: &AppConfig, certificate_pem: &str, private_key: &str) -> Result<()> {
    if let Some(parent) = config.mqtt.cert_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    if let Some(parent) = config.mqtt.key_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    fs::write(&config.mqtt.cert_path, certificate_pem)
        .with_context(|| format!("write {}", config.mqtt.cert_path.display()))?;
    fs::write(&config.mqtt.key_path, private_key)
        .with_context(|| format!("write {}", config.mqtt.key_path.display()))?;
    set_file_mode_600(&config.mqtt.cert_path)?;
    set_file_mode_600(&config.mqtt.key_path)?;
    Ok(())
}

fn load_config(path: &Path) -> Result<AppConfig> {
    let ini = Ini::load_from_file(path).with_context(|| format!("load config {}", path.display()))?;
    let sections = ini_to_map(&ini);
    Ok(AppConfig {
        aws: AwsSection {
            region: required(&sections, "aws", "region")?,
            mqtt_endpoint: required(&sections, "aws", "mqtt_endpoint")?,
            thing_name_prefix: required(&sections, "aws", "thing_name_prefix")?,
            thing_group_name: required(&sections, "aws", "thing_group_name")?,
            device_sn_source: required(&sections, "aws", "device_sn_source")?,
        },
        claim: ClaimSection {
            root_ca_path: PathBuf::from(required(&sections, "claim", "root_ca_path")?),
            cert_path: PathBuf::from(required(&sections, "claim", "cert_path")?),
            key_path: PathBuf::from(required(&sections, "claim", "key_path")?),
        },
        provisioning: ProvisioningSection {
            template_name: required(&sections, "provisioning", "template_name")?,
            thing_type_name: sections
                .get("provisioning")
                .and_then(|section| section.get("thing_type_name"))
                .cloned()
                .unwrap_or_else(|| "EdgeGateway".to_string()),
            connect_timeout_seconds: optional_u64(&sections, "provisioning", "connect_timeout_seconds", 30)?,
            provision_timeout_seconds: optional_u64(&sections, "provisioning", "provision_timeout_seconds", 60)?,
        },
        mqtt: MqttSection {
            port: optional_u16(&sections, "mqtt", "port", 8883)?,
            ca_path: PathBuf::from(required(&sections, "mqtt", "ca_path")?),
            cert_path: PathBuf::from(required(&sections, "mqtt", "cert_path")?),
            key_path: PathBuf::from(required(&sections, "mqtt", "key_path")?),
            connect_timeout_seconds: optional_u64(&sections, "mqtt", "connect_timeout_seconds", 30)?,
        },
        thing_sync: ThingSyncSection {
            check_aws_thing_existence: optional_bool(&sections, "thing_sync", "check_aws_thing_existence", false)?,
            allow_local_fallback_on_check_error: optional_bool(&sections, "thing_sync", "allow_local_fallback_on_check_error", true)?,
            credential_provider_endpoint: required(&sections, "thing_sync", "credential_provider_endpoint")?,
            role_alias: required(&sections, "thing_sync", "role_alias")?,
        },
        agent: AgentSection {
            session_state_path: PathBuf::from(required(&sections, "agent", "session_state_path")?),
            shell_path: required(&sections, "agent", "shell_path")?,
            default_session_timeout_minutes: optional_u64(&sections, "agent", "default_session_timeout_minutes", 10)?,
            max_session_timeout_minutes: optional_u64(&sections, "agent", "max_session_timeout_minutes", 1440)?,
            command_timeout_seconds: optional_u64(&sections, "agent", "command_timeout_seconds", 30)?,
            max_timeout_seconds: optional_u64(&sections, "agent", "max_timeout_seconds", 300)?,
            run_on_host: optional_bool(&sections, "agent", "run_on_host", false)?,
        },
        thing_tags: ThingTagsSection {
            tags: section_entries(&sections, "thing_tags"),
        },
    })
}

fn load_registration_state(path: &Path) -> Result<RegistrationState> {
    if !path.exists() {
        return Ok(RegistrationState::default());
    }
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    Ok(serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?)
}

fn write_json_file<T: Serialize>(path: &Path, payload: &T) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    let data = serde_json::to_vec_pretty(payload)?;
    fs::write(path, data).with_context(|| format!("write {}", path.display()))?;
    Ok(())
}

fn read_device_sn(path: &str) -> Result<String> {
    let data = fs::read(path).with_context(|| format!("read serial number from {path}"))?;
    let filtered = data.into_iter().filter(|byte| *byte != 0).collect::<Vec<_>>();
    Ok(String::from_utf8(filtered)?.trim().to_string())
}

fn now_string() -> String {
    Local::now().format("%Y-%m-%dT%H:%M:%S").to_string()
}

fn thing_create_time_string() -> String {
    let now = Local::now();
    let offset_seconds = now.offset().local_minus_utc();
    let sign = if offset_seconds >= 0 { "PLUS" } else { "MINUS" };
    let total_minutes = offset_seconds.abs() / 60;
    let hours = total_minutes / 60;
    let minutes = total_minutes % 60;
    format!(
        "{}_UTC_{}_{:02}:{:02}",
        now.format("%Y-%m-%dT%H:%M:%S"),
        sign,
        hours,
        minutes
    )
}

fn current_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or_default()
}

fn ini_to_map(ini: &Ini) -> HashMap<String, HashMap<String, String>> {
    let mut out = HashMap::new();
    for (section, properties) in ini.iter() {
        let section_name = section.unwrap_or_default().to_string();
        let mut entries = HashMap::new();
        for (key, value) in properties.iter() {
            entries.insert(key.to_string(), value.to_string());
        }
        out.insert(section_name, entries);
    }
    out
}

fn section_entries(
    sections: &HashMap<String, HashMap<String, String>>,
    section: &str,
) -> HashMap<String, String> {
    sections.get(section).cloned().unwrap_or_default()
}

fn required(
    sections: &HashMap<String, HashMap<String, String>>,
    section: &str,
    key: &str,
) -> Result<String> {
    sections
        .get(section)
        .and_then(|values| values.get(key))
        .cloned()
        .ok_or_else(|| anyhow!("missing config [{section}] {key}"))
}

fn optional_u64(
    sections: &HashMap<String, HashMap<String, String>>,
    section: &str,
    key: &str,
    default: u64,
) -> Result<u64> {
    match sections.get(section).and_then(|values| values.get(key)) {
        Some(value) => value.parse::<u64>().with_context(|| format!("parse [{section}] {key} as u64")),
        None => Ok(default),
    }
}

fn optional_u16(
    sections: &HashMap<String, HashMap<String, String>>,
    section: &str,
    key: &str,
    default: u16,
) -> Result<u16> {
    match sections.get(section).and_then(|values| values.get(key)) {
        Some(value) => value.parse::<u16>().with_context(|| format!("parse [{section}] {key} as u16")),
        None => Ok(default),
    }
}

fn optional_bool(
    sections: &HashMap<String, HashMap<String, String>>,
    section: &str,
    key: &str,
    default: bool,
) -> Result<bool> {
    match sections.get(section).and_then(|values| values.get(key)) {
        Some(value) => match value.to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Ok(true),
            "false" | "0" | "no" | "off" => Ok(false),
            _ => bail!("parse [{section}] {key} as bool"),
        },
        None => Ok(default),
    }
}

#[cfg(unix)]
fn set_file_mode_600(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let permissions = fs::Permissions::from_mode(0o600);
    fs::set_permissions(path, permissions).with_context(|| format!("chmod 600 {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_file_mode_600(_path: &Path) -> Result<()> {
    Ok(())
}
