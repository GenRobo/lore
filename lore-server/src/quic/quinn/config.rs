// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::fmt;
use std::fmt::Formatter;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::anyhow;
use lore_transport::tls;
use rustls::server::NoClientAuth;
use rustls::server::danger::ClientCertVerifier;
use tracing::info;

use crate::quic::StreamHandlerFactory;

pub struct QuinnConfig {
    // In observability, what is the label that distinguishes this Quinn
    // server from others?
    pub(crate) server_metrics_name: String,
    pub(crate) address: SocketAddr,
    pub(crate) alpns: Vec<String>,
    pub(crate) cert_file: PathBuf,
    pub(crate) pkey_file: PathBuf,
    pub(crate) cert_chain: Option<PathBuf>,
    pub(crate) client_cert_verifier: Arc<dyn ClientCertVerifier>,
    pub(crate) stream_handler_factory: Box<dyn StreamHandlerFactory>,
    pub(crate) idle_timeout: Duration,
    pub(crate) keep_alive: Duration,
    pub(crate) max_bidi_streams: u64,
    pub(crate) num_listeners: u8,
    pub(crate) metrics_frequency: Duration,
    pub(crate) transport_bits_per_second: usize,
    pub(crate) transport_rtt: usize,
    pub(crate) max_connections: Option<usize>,
    pub(crate) connection_wait: Duration,
}

impl fmt::Debug for QuinnConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QuinnConfig")
            .field("address", &self.address)
            .field("alpns", &self.alpns)
            .field("cert_file", &self.cert_file)
            .field("pkey_file", &self.pkey_file)
            .field("cert_chain", &self.cert_chain)
            .field(
                "stream_handler_factory",
                &self.stream_handler_factory.name(),
            )
            .field("idle_timeout", &self.idle_timeout)
            .field("keep_alive", &self.keep_alive)
            .field("max_bidi_streams", &self.max_bidi_streams)
            .field("num_listeners", &self.num_listeners)
            .field("metrics_frequency", &self.metrics_frequency)
            .field("transport_bits_per_second", &self.transport_bits_per_second)
            .field("transport_rtt", &self.transport_rtt)
            .field("max_connections", &self.max_connections)
            .field("connection_wait", &self.connection_wait)
            .finish()
    }
}

#[derive(Default)]
pub struct QuinnConfigBuilder {
    // In observability, what is the label that distinguishes this Quinn
    // server from others?
    pub(crate) server_metrics_name: String,
    pub(crate) address: Option<SocketAddr>,
    pub(crate) cert_file: PathBuf,
    pub(crate) pkey_file: PathBuf,
    pub(crate) cert_chain: Option<PathBuf>,
    pub(crate) client_cert_verifier: Option<Arc<dyn ClientCertVerifier>>,
    pub(crate) stream_handler_factory: Option<Box<dyn StreamHandlerFactory>>,
    pub(crate) idle_timeout: Option<Duration>,
    pub(crate) keep_alive: Option<Duration>,
    pub(crate) max_bidi_streams: Option<u64>,
    pub(crate) num_listeners: Option<u8>,
    pub(crate) metrics_frequency: Option<Duration>,
    pub(crate) transport_bits_per_second: Option<usize>,
    pub(crate) transport_rtt: Option<usize>,
    pub(crate) max_connections: Option<usize>,
    pub(crate) connection_wait: Option<Duration>,
}

const DEFAULT_IDLE_TIMEOUT_MILLIS: u64 = 30_000;
const DEFAULT_KEEP_ALIVE_MILLS: u64 = 500;
const DEFAULT_BIDI_STREAMS: u64 = 8;
const DEFAULT_LISTENERS_COUNT: u8 = 10;
const DEFAULT_METRICS_FREQUENCY: Duration = Duration::from_millis(60_000);
const DEFAULT_TRANSPORT_BITS_PER_SECOND: usize = 1_073_741_824; // 1gbit/s
const DEFAULT_TRANSPORT_RTT: usize = 100; // 100ms
// A client gives its handshake up after 5 s (lore-transport, HANDSHAKE_TIMEOUT_SECS) and cannot
// tell a server that makes it wait from one that is not there: the wait has to end before that,
// for the client to be told that it was refused.
const DEFAULT_CONNECTION_WAIT: Duration = Duration::from_secs(3);

impl QuinnConfigBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn server_metrics_name(mut self, name: &str) -> Self {
        self.server_metrics_name = name.into();
        self
    }

    pub fn address(mut self, address: SocketAddr) -> Self {
        self.address = Some(address);
        self
    }

    pub fn cert_chain(mut self, cert_chain: Option<PathBuf>) -> Self {
        self.cert_chain = cert_chain;
        self
    }

    pub fn cert_file(mut self, cert_file: PathBuf) -> Self {
        self.cert_file = cert_file;
        self
    }

    pub fn pkey_file(mut self, pkey_file: PathBuf) -> Self {
        self.pkey_file = pkey_file;
        self
    }

    pub fn client_cert_verifier(mut self, verifier: Arc<dyn ClientCertVerifier>) -> Self {
        self.client_cert_verifier = Some(verifier);
        self
    }

    pub fn stream_handler_factory(
        mut self,
        stream_handler_factory: Box<dyn StreamHandlerFactory>,
    ) -> Self {
        self.stream_handler_factory = Some(stream_handler_factory);
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    pub fn keep_alive(mut self, keep_alive: Duration) -> Self {
        self.keep_alive = Some(keep_alive);
        self
    }

    pub fn max_bidi_streams(mut self, max_streams: u64) -> Self {
        self.max_bidi_streams = Some(max_streams);
        self
    }

    pub fn num_listeners(mut self, num_listeners: u8) -> Self {
        self.num_listeners = Some(num_listeners);
        self
    }

    pub fn metrics_frequency(mut self, metric_frequency: Duration) -> Self {
        self.metrics_frequency = Some(metric_frequency);
        self
    }

    pub fn transport_bits_per_second(mut self, bits_per_second: usize) -> Self {
        self.transport_bits_per_second = Some(bits_per_second);
        self
    }

    pub fn transport_rtt(mut self, rtt: usize) -> Self {
        self.transport_rtt = Some(rtt);
        self
    }

    /// Keep at most this many connections open. Past it a new connection waits for a place
    /// (`connection_wait`) and is refused when none comes free. Unset: no limit.
    pub fn max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = Some(max_connections);
        self
    }

    /// How long a connection past `max_connections` waits for a place before it is refused.
    pub fn connection_wait(mut self, connection_wait: Duration) -> Self {
        self.connection_wait = Some(connection_wait);
        self
    }

    pub fn build(self) -> anyhow::Result<QuinnConfig> {
        let stream_handler_factory = self
            .stream_handler_factory
            .ok_or(anyhow!("Stream handler factory was not set"))?;

        let alpns = stream_handler_factory.supported_protocols();
        if alpns.is_empty() {
            return Err(anyhow!("No alpns provided"));
        };

        Ok(QuinnConfig {
            server_metrics_name: self.server_metrics_name,
            address: self.address.ok_or(anyhow!("Address was not set"))?,
            alpns,
            cert_file: self.cert_file,
            pkey_file: self.pkey_file,
            cert_chain: self.cert_chain,
            client_cert_verifier: self
                .client_cert_verifier
                .unwrap_or(Arc::new(NoClientAuth {})),
            stream_handler_factory,
            idle_timeout: self
                .idle_timeout
                .unwrap_or(Duration::from_millis(DEFAULT_IDLE_TIMEOUT_MILLIS)),
            keep_alive: self
                .keep_alive
                .unwrap_or(Duration::from_millis(DEFAULT_KEEP_ALIVE_MILLS)),
            max_bidi_streams: self.max_bidi_streams.unwrap_or(DEFAULT_BIDI_STREAMS),
            num_listeners: self.num_listeners.unwrap_or(DEFAULT_LISTENERS_COUNT),
            metrics_frequency: self.metrics_frequency.unwrap_or(DEFAULT_METRICS_FREQUENCY),
            transport_bits_per_second: self
                .transport_bits_per_second
                .unwrap_or(DEFAULT_TRANSPORT_BITS_PER_SECOND),
            transport_rtt: self.transport_rtt.unwrap_or(DEFAULT_TRANSPORT_RTT),
            max_connections: self.max_connections,
            connection_wait: self.connection_wait.unwrap_or(DEFAULT_CONNECTION_WAIT),
        })
    }
}

pub(crate) fn crypto_config(config: &QuinnConfig) -> anyhow::Result<rustls::ServerConfig> {
    let mut certs = tls::load_certs(&config.cert_file)?;
    if let Some(chain_path) = &config.cert_chain {
        let chain_certs = tls::load_certs(chain_path)?;
        certs.extend(chain_certs);
    }

    let private_key = tls::load_private_key(&config.pkey_file)?;

    let mut server_config =
        rustls::ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_client_cert_verifier(config.client_cert_verifier.clone())
            .with_single_cert(certs, private_key)?;

    server_config.alpn_protocols = config
        .alpns
        .iter()
        .map(|alpn| {
            info!("Configuring ALPN: {alpn}");
            alpn.as_bytes().into()
        })
        .collect();

    Ok(server_config)
}
