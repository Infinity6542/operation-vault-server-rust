use anyhow::{Context, Result};
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    Client as S3Client,
    config::{Builder as S3ConfigBuilder, Region},
    primitives::ByteStream,
};
// use include_dir::{Dir, include_dir};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;
use wtransport::{Endpoint, RecvStream, SendStream, ServerConfig, endpoint::endpoint_side::Server};

// static EMBED_CLIENT: Dir<'_> = include_dir!("$CARGO_MANIFEST_DIR/embed_client");
const BUCKET_NAME: &str = "opvault-test";
const ADDR: &str = "127.0.0.1:8080";

struct Client {
    stream: Arc<tokio::sync::Mutex<SendStream>>,
    peer_id: String,
    nickname: String,
    last_seen: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    r#type: String,
    channel_id: String,
    payload: String,
    sender_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nickname: Option<String>,
}

struct Hub {
    channels: HashMap<String, HashMap<String, Client>>,
}

impl Hub {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
        }
    }
}

#[derive(Clone)]
struct State {
    hub: Arc<RwLock<Hub>>,
    file_owners: Arc<RwLock<HashMap<String, String>>>,
    s3_client: Arc<S3Client>,
}

async fn handle_web_transport(conn: wtransport::Connection, state: State) {
    loop {
        match conn.accept_bi().await {
            Ok((send, recv)) => {
                let state = state.clone();
                tokio::spawn(handle_stream(send, recv, state));
            }
            Err(e) => {
                error!("Couldn't accept stream due to: {}", e);
                return;
            }
        }
    }
}

async fn handle_stream(mut send: SendStream, mut recv: RecvStream, state: State) {
    let msg = match read_message(&mut recv).await {
        Ok(m) => m,
        Err(e) => {
            error!("Couldn't decode the mnessage: {}", e);
            return;
        }
    };

    let channel = msg.channel_id.clone();

    match msg.r#type.as_str() {
        "upload" => {
            info!("Upload request for: {}", msg.payload);
            let data = read_remaining(&mut recv).await.unwrap_or_default();
            match upload(
                &state.s3_client,
                &data,
                &channel,
                &msg.payload,
                &msg.sender_id,
            )
            .await
            {
                Ok(()) => {
                    state
                        .file_owners
                        .write()
                        .await
                        .insert(msg.payload.clone(), msg.sender_id.clone());
                    info!("Uploaded {} for {}", msg.payload, msg.sender_id);
                }
                Err(e) => {
                    error!("Failed to upload {} because {}", msg.payload, e);
                }
            }
            return;
        }
        "download" => {
            download(&state.s3_client, &mut send, &channel, &msg.payload).await;
            return;
        }
        "remove" => {
            let owner = state.file_owners.read().await.get(&msg.payload).cloned();
            let owner = match owner {
                Some(o) => Some(o),
                None => {
                    let found = get_owner(&state.s3_client, &msg.payload).await;
                    if let Some(ref o) = found {
                        state
                            .file_owners
                            .write()
                            .await
                            .insert(msg.payload.clone(), o.clone());
                    }
                    found
                }
            };

            match owner {
                None => {
                    warn!("No recorded owner for {}, permitting deletion", msg.payload);

                    let _ = remove(&state.s3_client, &msg.payload).await;
                }
                Some(ref o) if o == &msg.sender_id => {
                    let _ = remove(&state.s3_client, &msg.payload).await;
                    state.file_owners.write().await.remove(&msg.payload);
                }
                Some(ref o) => {
                    warn!(
                        "{} is not permitted to delete {}, owned by {}",
                        msg.sender_id, msg.payload, o
                    );
                }
            }
            return;
        }
        "join" => {
            info!(
                "Client {} joining channel {}",
                msg.sender_id, msg.channel_id
            );
            register_client(&state, &msg, send).await;
        }
        _ => {
            broadcast(&state, &msg, &msg.sender_id).await;
        }
    }

    loop {
        let msg = match read_message(&mut recv).await {
            Ok(m) => m,
            Err(e) => {
                if e.to_string().contains("EOF") || e.to_string().contains("closed") {
                    error!("Stream unexpectedly closed by client: {}", e);
                } else {
                    error!("Error decoding message: {}", e);
                }
                break;
            }
        };

        {
            let mut hub = state.hub.write().await;
            if let Some(clients) = hub.channels.get_mut(&msg.channel_id) {
                if let Some(client) = clients.get_mut(&msg.sender_id) {
                    client.last_seen = Instant::now();
                }
            }
        }

        match msg.r#type.as_str() {
            "join" => {
                info!(
                    "Client {} ({}) joining channel {}",
                    msg.sender_id,
                    msg.nickname.as_deref().unwrap_or(""),
                    msg.channel_id
                );
                {
                    let mut hub = state.hub.write().await;
                    hub.channels
                        .entry(msg.channel_id.clone())
                        .or_insert_with(HashMap::new)
                        .entry(msg.sender_id.clone())
                        .and_modify(|c| c.last_seen = Instant::now());
                }
                broadcast_user_list(&state, &msg.channel_id).await;
            }
            "message" => {}
            "heartbeat" => {}
            "leave" => {
                info!(
                    "Client {} ({}) leaving channel {}",
                    msg.sender_id,
                    msg.nickname.as_deref().unwrap_or(""),
                    msg.channel_id
                );

                {
                    let mut hub = state.hub.write().await;
                    if let Some(clients) = hub.channels.get_mut(&msg.channel_id) {
                        clients.remove(&msg.sender_id);
                   }
                }
                broadcast_user_list(&state, &msg.channel_id).await;
                return;
            }
            _ => {
                info!(
                    "Message of type '{}' received for channel {}",
                    msg.r#type, msg.channel_id
                );
                // Potentially unsafe
                broadcast(&state, &msg, &msg.sender_id).await;
            }
        }
    }

    {
        let mut hub = state.hub.write().await;
        if let Some(clients) = hub.channels.get_mut(&channel) {
            clients.remove(&msg.sender_id);
        }
    }
    broadcast_user_list(&state, &channel).await;
}

async fn register_client(state: &State, msg: &Message, send: SendStream) {
    let stream = Arc::new(tokio::sync::Mutex::new(send));

    {
        let mut hub = state.hub.write().await;
        hub.channels
            .entry(msg.channel_id.clone())
            .or_insert_with(HashMap::new)
            .insert(
                msg.sender_id.clone(),
                Client {
                    stream,
                    peer_id: msg.sender_id.clone(),
                    nickname: msg.nickname.clone().unwrap_or_default(),
                    last_seen: Instant::now(),
                },
            );
    }

    broadcast_user_list(state, &msg.channel_id).await;
}

async fn broadcast(state: &State, msg: &Message, sender_id: &str) {
    let hub = state.hub.read().await;
    let Some(clients) = hub.channels.get(&msg.channel_id) else {
        warn!(
            "No clients in channel {} to broadcast message.",
            msg.channel_id
        );
        return;
    };

    let mut data = serde_json::to_vec(&msg).unwrap_or_default();
    data.push(b'\n');

    for (peer_id, client) in clients {
        if peer_id == sender_id {
            continue;
        }

        let mut stream: tokio::sync::MutexGuard<'_, SendStream> = client.stream.lock().await;
        if let Err(e) = stream.write_all(&data).await {
            error!("Error broadcasting to stream {}: {}", peer_id, e);
        }
    }
}

// fn serve_embedded(path: &str) -> Option<(&'static [u8], &'static str)> {
//     let path = path.trim_start_matches('/');
//     let path = if path.is_empty() { "index.html" } else { path };
//     let file = EMBED_CLIENT.get_file(path)?;
//     Some((file.contents(), mime_type(path)))
// }
//
// fn mime_type(path: &str) -> &'static str {
//     match path.rsplit('.').next().unwrap_or("") {
//         "html" => "text/html; charset=utf-8",
//         "js" | "mjs" => "application/javascript",
//         "css" => "text/css",
//         "json" => "application/json",
//         "png" => "image/png",
//         "svg" => "image/svg+xml",
//         "ico" => "image/x-icon",
//         "wasm" => "application/wasm",
//         _ => "application/octet-stream",
//     }
// }

fn cert_handler() -> Result<(wtransport::Identity, String)> {
    let identity = wtransport::Identity::self_signed(["localhost", "127.0.0.1"])
        .context("Failed to generate self-signed certificate")?;

    // Sha256Digest implements AsRef<[u8; 32]>, giving us the raw bytes to
    // base64-encode — matching Go's base64.StdEncoding.EncodeToString(sha256sum[:])
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
    let digest = identity.certificate_chain().as_slice()[0].hash();
    let fingerprint = BASE64.encode(digest.as_ref());

    Ok((identity, fingerprint))
}

async fn init_s3() -> Result<S3Client> {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let s3_config = S3ConfigBuilder::from(&sdk_config)
        .endpoint_url("https://52734793e62aadf91e3bc988c6d667cc.eu.r2.cloudflarestorage.com")
        .region(Region::new("auto"))
        .build();
    let client = S3Client::from_conf(s3_config);
    info!("S3 client initialised");
    Ok(client)
}

async fn upload(
    client: &S3Client,
    data: &[u8],
    channel: &str,
    file_id: &str,
    owner_id: &str,
) -> Result<()> {
    client
        .put_object()
        .bucket(BUCKET_NAME)
        .key(format!("{}/{}", channel, file_id))
        .body(ByteStream::from(data.to_vec()))
        .metadata("owner", owner_id)
        .send()
        .await
        .context("Failed to upload to S3")?;

    info!("File {} uploaded to bucket {}", file_id, BUCKET_NAME);
    Ok(())
}

async fn get_owner(client: &S3Client, file_id: &str) -> Option<String> {
    let r = client
        .head_object()
        .bucket(BUCKET_NAME)
        .key(format!("{}/manifest.json", file_id))
        .send()
        .await
        .ok()?;

    r.metadata()?.get("owner").cloned()
}

async fn download(client: &S3Client, send: &mut SendStream, channel: &str, file_id: &str) {
    info!("Downloading file {} from bucket {}", file_id, BUCKET_NAME);

    let r = match client
        .get_object()
        .bucket(BUCKET_NAME)
        .key(format!("{}/{}", channel, file_id))
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to download file from S3: {:#?}", e);
            return;
        }
    };

    let mut body = r.body.into_async_read();
    match tokio::io::copy(&mut body, send).await {
        Ok(n) => info!("File {} ({} bytes) successfully sent to client", file_id, n),
        Err(e) => error!("Error while sending file to client: {}", e),
    }
}

async fn remove(client: &S3Client, channel_id: &str) -> Result<()> {
    let list = client
        .list_objects_v2()
        .bucket(BUCKET_NAME)
        .prefix(format!("{}/", channel_id))
        .send()
        .await
        .context("Failed to list objects for deletion")?;

    for obj in list.contents() {
        let key = obj.key().unwrap_or_default();
        match client
            .delete_object()
            .bucket(BUCKET_NAME)
            .key(key)
            .send()
            .await
        {
            Ok(_) => info!("Deleted object {}", key),
            Err(e) => error!("Failed to delete object {}: {}", key, e),
        }
    }

    info!("Channel folder {} deleted from {}", channel_id, BUCKET_NAME);
    Ok(())
}

async fn cleanup_loop(state: State) -> Result<()> {
    let mut ticker = tokio::time::interval(Duration::from_secs(20));
    loop {
        ticker.tick().await;

        let mut changed: Vec<String> = Vec::new();

        {
            let mut hub = state.hub.write().await;
            let channel_ids: Vec<String> = hub.channels.keys().cloned().collect();

            for channel_id in &channel_ids {
                let clients = hub.channels.get_mut(channel_id).unwrap();
                let before = clients.len();

                clients.retain(|peer_id, client| {
                    if client.last_seen.elapsed() > Duration::from_secs(20) {
                        info!(
                            "Removing inactive client {} from channel {}",
                            peer_id, channel_id
                        );
                        false
                    } else {
                        true
                    }
                });

                if clients.is_empty() {
                    info!("Removing empty channel {}", channel_id);
                    hub.channels.remove(channel_id);
                } else if clients.len() != before {
                    changed.push(channel_id.clone());
                }
            }
        }

        for channel_id in changed {
            broadcast_user_list(&state, &channel_id).await;
        }
    }
}

async fn broadcast_user_list(state: &State, channel_id: &str) {
    let hub = state.hub.read().await;
    let Some(clients) = hub.channels.get(channel_id) else {
        return;
    };

    let user_list: HashMap<&str, &str> = clients
        .values()
        .map(|c| (c.peer_id.as_str(), c.nickname.as_str()))
        .collect();

    let payload = match serde_json::to_string(&user_list) {
        Ok(p) => p,
        Err(e) => {
            error!("Failed to marshal user list: {}", e);
            return;
        }
    };

    let msg = Message {
        r#type: "user_list".into(),
        channel_id: channel_id.to_string(),
        sender_id: "Server".into(),
        payload,
        nickname: None,
    };

    let mut data = serde_json::to_vec(&msg).unwrap_or_default();
    data.push(b'\n');

    for client in clients.values() {
        let mut stream: tokio::sync::MutexGuard<'_, SendStream> = client.stream.lock().await;
        if let Err(e) = stream.write_all(&data).await {
            error!(
                "Error broadcasting user list to stream {}: {}",
                client.peer_id, e
            );
        }
    }
}

async fn read_message(recv: &mut RecvStream) -> Result<Message> {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];

    loop {
        let n = recv
            .read(&mut byte)
            .await
            .context("Stream read error")?
            .ok_or_else(|| anyhow::anyhow!("Stream closed (EOF)"))?;

        if n == 0 || byte[0] == b'\n' {
            break;
        }
        buf.push(byte[0]);
    }

    if buf.is_empty() {
        return Err(anyhow::anyhow!("Stream closed (EOF)"));
    }

    serde_json::from_slice::<Message>(&buf).context("JSON decode error")
}

async fn read_remaining(recv: &mut RecvStream) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    let mut chunk = [0u8; 8192];

    loop {
        match recv.read(&mut chunk).await? {
            Some(n) if n > 0 => data.extend_from_slice(&chunk[..n]),
            _ => break,
        }
    }

    Ok(data)
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let addr: SocketAddr = ADDR.parse().unwrap();
    let (identity, fingerprint) = cert_handler()?;
    info!("Starting server at https://{}", addr);
    info!("Certificate hash: '{}'", fingerprint);
    info!("Initialising S3 client...");

    let s3_client = Arc::new(init_s3().await?);

    let state = State {
        hub: Arc::new(RwLock::new(Hub::new())),
        file_owners: Arc::new(RwLock::new(HashMap::new())),
        s3_client,
    };

    let config = ServerConfig::builder()
        .with_bind_address(addr)
        .with_identity(identity)
        .build();

    let endpoint: Endpoint<Server> =
        Endpoint::server(config).context("Failed to start WebTransport endpoint")?;

    info!("Listening on UDP {}", addr);

    {
        let state = state.clone();
        tokio::spawn(async move { cleanup_loop(state).await });
    }

    loop {
        let incoming = endpoint.accept().await;
        let state = state.clone();
        tokio::spawn(async move {
            match incoming.await {
                Ok(req) => match req.accept().await {
                    Ok(conn) => {
                        info!("Session from {} accepted", conn.remote_address());
                        handle_web_transport(conn, state).await;
                    }
                    Err(e) => error!("Failed to accept session request: {}", e),
                },
                Err(e) => error!("Failed to establish session: {}", e),
            }
        });
    }
}
