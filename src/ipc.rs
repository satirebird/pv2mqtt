// SPDX-License-Identifier: Apache-2.0

//! Local control interface: line-delimited JSON over a Unix domain socket.
//!
//! Request:  `{"serial":"SN123","cmd":"WMaxLimPct","value":50}`
//! Response: `{"ok":true}` or `{"ok":false,"error":"..."}`

use crate::commands::{CommandReply, ControlAction, ModbusCommand};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::os::unix::fs::{FileTypeExt, PermissionsExt};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Semaphore, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

pub const IPC_MAX_LINE_LENGTH: usize = 1024;
pub const IPC_MAX_CLIENTS: usize = 8;
pub const IPC_REPLY_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Deserialize)]
struct IpcRequest {
    serial: String,
    cmd: String,
    value: serde_json::Value,
}

#[derive(Debug, Serialize, PartialEq)]
struct IpcResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

impl IpcResponse {
    fn ok() -> Self {
        Self {
            ok: true,
            error: None,
        }
    }

    fn error(msg: impl Into<String>) -> Self {
        Self {
            ok: false,
            error: Some(msg.into()),
        }
    }
}

pub struct IpcTask {
    path: PathBuf,
    mode: u32,
    cmd_tx: broadcast::Sender<ModbusCommand>,
    token: CancellationToken,
}

impl IpcTask {
    pub fn new(
        path: PathBuf,
        mode: u32,
        cmd_tx: broadcast::Sender<ModbusCommand>,
        token: CancellationToken,
    ) -> Self {
        Self {
            path,
            mode,
            cmd_tx,
            token,
        }
    }

    // Binds the socket, removing a stale socket file left over from a previous run.
    fn bind(&self) -> Result<UnixListener> {
        match std::fs::symlink_metadata(&self.path) {
            Ok(meta) if meta.file_type().is_socket() => {
                debug!("Removing stale control socket {:?}", self.path);
                std::fs::remove_file(&self.path)?;
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::AlreadyExists,
                    format!("{:?} exists and is not a socket", self.path),
                )
                .into());
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        let listener = UnixListener::bind(&self.path)?;
        std::fs::set_permissions(&self.path, std::fs::Permissions::from_mode(self.mode))?;
        Ok(listener)
    }

    #[tracing::instrument(name="ipc", skip(self), fields(path=?self.path))]
    pub async fn run(self) -> Result<()> {
        let listener = self.bind()?;
        info!("Control socket listening");

        let clients = Arc::new(Semaphore::new(IPC_MAX_CLIENTS));

        loop {
            tokio::select! {
                biased;
                _ = self.token.cancelled() => break,
                res = listener.accept() => {
                    let mut stream = match res {
                        Ok((stream, _)) => stream,
                        Err(e) => {
                            error!("Failed to accept control connection: {}", e);
                            continue;
                        }
                    };
                    let Ok(permit) = Arc::clone(&clients).try_acquire_owned() else {
                        warn!("Rejected control connection: too many clients");
                        let _ = write_response(&mut stream, &IpcResponse::error("too many clients")).await;
                        continue;
                    };
                    let cmd_tx = self.cmd_tx.clone();
                    let token = self.token.clone();
                    tokio::spawn(async move {
                        handle_client(stream, cmd_tx, token).await;
                        drop(permit);
                    });
                }
            }
        }

        if let Err(e) = std::fs::remove_file(&self.path) {
            warn!("Failed to remove control socket: {}", e);
        }
        info!("Control socket closed");
        Ok(())
    }
}

async fn handle_client(
    stream: UnixStream,
    cmd_tx: broadcast::Sender<ModbusCommand>,
    token: CancellationToken,
) {
    let (reader, mut writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut buf = Vec::with_capacity(256);

    debug!("Control client connected");
    loop {
        let line = tokio::select! {
            biased;
            _ = token.cancelled() => break,
            line = read_line(&mut reader, &mut buf) => line,
        };

        let response = match line {
            Ok(Some(line)) if line.trim().is_empty() => continue,
            Ok(Some(line)) => process_request(line, &cmd_tx).await,
            Ok(None) => break,
            Err(e) => {
                warn!("Control client error: {}", e);
                let _ = write_response(&mut writer, &IpcResponse::error(e.to_string())).await;
                break;
            }
        };

        if let Err(e) = write_response(&mut writer, &response).await {
            debug!("Failed to write control response: {}", e);
            break;
        }
    }
    debug!("Control client disconnected");
}

// Reads one newline-terminated line of at most IPC_MAX_LINE_LENGTH bytes.
// Returns Ok(None) on EOF.
async fn read_line<'a, R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    buf: &'a mut Vec<u8>,
) -> std::io::Result<Option<&'a str>> {
    buf.clear();
    let limit = IPC_MAX_LINE_LENGTH as u64 + 1;
    let n = reader.take(limit).read_until(b'\n', buf).await?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() == Some(&b'\n') {
        buf.pop();
    } else if buf.len() > IPC_MAX_LINE_LENGTH {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "request line too long",
        ));
    }
    std::str::from_utf8(buf)
        .map(Some)
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid UTF-8"))
}

async fn write_response<W: AsyncWrite + Unpin>(
    writer: &mut W,
    response: &IpcResponse,
) -> std::io::Result<()> {
    let mut out = serde_json::to_vec(response)?;
    out.push(b'\n');
    writer.write_all(&out).await
}

async fn process_request(line: &str, cmd_tx: &broadcast::Sender<ModbusCommand>) -> IpcResponse {
    let req: IpcRequest = match serde_json::from_str(line) {
        Ok(r) => r,
        Err(e) => return IpcResponse::error(format!("invalid request: {}", e)),
    };

    let value = match &req.value {
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::String(s) => s.trim().to_string(),
        _ => return IpcResponse::error("value must be a boolean, number or string"),
    };

    let Some(action) = ControlAction::parse(&req.cmd, &value) else {
        return IpcResponse::error(format!(
            "unknown command or invalid value: {} = {}",
            req.cmd, value
        ));
    };

    info!("Control request for {}: {:?}", req.serial, action);

    let (reply, rx) = CommandReply::new();
    let cmd = ModbusCommand {
        serial: req.serial,
        action,
        reply: Some(reply),
    };
    if cmd_tx.send(cmd).is_err() {
        return IpcResponse::error("no connection tasks running");
    }

    match tokio::time::timeout(Duration::from_secs(IPC_REPLY_TIMEOUT_SECS), rx).await {
        Ok(Ok(Ok(()))) => IpcResponse::ok(),
        Ok(Ok(Err(e))) => IpcResponse::error(e),
        Ok(Err(_)) => IpcResponse::error("unknown serial or device not available"),
        Err(_) => IpcResponse::error("timeout waiting for command result"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncBufReadExt;

    fn temp_socket_path() -> PathBuf {
        static COUNTER: AtomicUsize = AtomicUsize::new(0);
        std::env::temp_dir().join(format!(
            "pv2mqtt-test-{}-{}.sock",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ))
    }

    struct Harness {
        path: PathBuf,
        token: CancellationToken,
        handle: tokio::task::JoinHandle<Result<()>>,
    }

    async fn start(cmd_tx: broadcast::Sender<ModbusCommand>) -> Harness {
        let path = temp_socket_path();
        let token = CancellationToken::new();
        let task = IpcTask::new(path.clone(), 0o600, cmd_tx, token.clone());
        let handle = tokio::spawn(task.run());
        // Wait for the socket to appear
        for _ in 0..100 {
            if path.exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        Harness {
            path,
            token,
            handle,
        }
    }

    impl Harness {
        async fn stop(self) {
            self.token.cancel();
            self.handle.await.unwrap().unwrap();
            assert!(!self.path.exists(), "socket file should be removed");
        }
    }

    async fn request(stream: &mut BufReader<UnixStream>, line: &str) -> serde_json::Value {
        stream.get_mut().write_all(line.as_bytes()).await.unwrap();
        let mut resp = String::new();
        stream.read_line(&mut resp).await.unwrap();
        serde_json::from_str(&resp).unwrap()
    }

    #[tokio::test]
    async fn test_ipc_command_roundtrip() {
        let (cmd_tx, mut cmd_rx) = broadcast::channel::<ModbusCommand>(4);
        let h = start(cmd_tx).await;

        // Simulated connection task answering commands
        let responder = tokio::spawn(async move {
            let cmd = cmd_rx.recv().await.unwrap();
            assert_eq!(cmd.serial, "SN123");
            assert!(matches!(cmd.action, ControlAction::WMaxLimPct(v) if v == 50.0));
            cmd.respond(Ok(()));

            let cmd = cmd_rx.recv().await.unwrap();
            assert!(matches!(cmd.action, ControlAction::Conn(false)));
            cmd.respond(Err("Device not connected".to_string()));
        });

        let mut stream = BufReader::new(UnixStream::connect(&h.path).await.unwrap());
        let resp = request(
            &mut stream,
            "{\"serial\":\"SN123\",\"cmd\":\"WMaxLimPct\",\"value\":50}\n",
        )
        .await;
        assert_eq!(resp, serde_json::json!({"ok": true}));

        let resp = request(
            &mut stream,
            "{\"serial\":\"SN123\",\"cmd\":\"Conn\",\"value\":\"off\"}\n",
        )
        .await;
        assert_eq!(
            resp,
            serde_json::json!({"ok": false, "error": "Device not connected"})
        );

        responder.await.unwrap();
        drop(stream);
        h.stop().await;
    }

    #[tokio::test]
    async fn test_ipc_invalid_requests() {
        let (cmd_tx, mut cmd_rx) = broadcast::channel::<ModbusCommand>(4);
        let h = start(cmd_tx).await;
        let mut stream = BufReader::new(UnixStream::connect(&h.path).await.unwrap());

        let resp = request(&mut stream, "not json\n").await;
        assert_eq!(resp["ok"], false);

        let resp = request(
            &mut stream,
            "{\"serial\":\"SN123\",\"cmd\":\"Foo\",\"value\":1}\n",
        )
        .await;
        assert_eq!(resp["ok"], false);

        let resp = request(
            &mut stream,
            "{\"serial\":\"SN123\",\"cmd\":\"Conn\",\"value\":[1]}\n",
        )
        .await;
        assert_eq!(resp["ok"], false);

        // No command must have been forwarded
        assert!(cmd_rx.try_recv().is_err());

        // Oversized line closes the connection after an error response
        let long = format!("{}\n", "x".repeat(IPC_MAX_LINE_LENGTH + 10));
        let resp = request(&mut stream, &long).await;
        assert_eq!(resp["error"], "request line too long");

        drop(stream);
        h.stop().await;
    }

    #[tokio::test]
    async fn test_ipc_unknown_serial() {
        let (cmd_tx, mut cmd_rx) = broadcast::channel::<ModbusCommand>(4);
        let h = start(cmd_tx).await;

        // Receiver that ignores the command (not its device) and drops it
        let ignorer = tokio::spawn(async move {
            let _ = cmd_rx.recv().await.unwrap();
            drop(cmd_rx);
        });

        let mut stream = BufReader::new(UnixStream::connect(&h.path).await.unwrap());
        let resp = request(
            &mut stream,
            "{\"serial\":\"NOPE\",\"cmd\":\"WMaxLim_Ena\",\"value\":true}\n",
        )
        .await;
        assert_eq!(
            resp,
            serde_json::json!({"ok": false, "error": "unknown serial or device not available"})
        );

        ignorer.await.unwrap();
        drop(stream);
        h.stop().await;
    }

    #[tokio::test]
    async fn test_ipc_refuses_to_replace_regular_file() {
        let path = temp_socket_path();
        std::fs::write(&path, b"data").unwrap();
        let (cmd_tx, _) = broadcast::channel::<ModbusCommand>(1);
        let task = IpcTask::new(path.clone(), 0o600, cmd_tx, CancellationToken::new());
        assert!(task.run().await.is_err());
        assert!(path.exists());
        std::fs::remove_file(&path).unwrap();
    }
}
