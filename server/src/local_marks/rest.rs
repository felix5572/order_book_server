//! The two REST calls the feeds need (design 000454 r1 1.2): MEXC's spot book tickers (its spot WS is protobuf) and
//! Kucoin's public WS token. HTTPS over the TLS stack `yawc` already brings (tokio-rustls, ring, webpki roots), one
//! kept-alive HTTP/1.1 connection per client.

use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper::{Method, Request, StatusCode, client::conn::http1::SendRequest, header};
use hyper_util::rt::TokioIo;
use tokio::net::TcpStream;
use tokio_rustls::{
    TlsConnector,
    rustls::{self, pki_types::ServerName},
};

use super::connection::FeedError;

const TIMEOUT: Duration = Duration::from_secs(5);
/// Retried after a backoff: the request may succeed later.
const TRANSIENT_STATUS: [u16; 6] = [408, 429, 500, 502, 503, 504];

/// One host's HTTPS client.
pub(crate) struct RestClient {
    host: &'static str,
    tls: TlsConnector,
    sender: Option<SendRequest<Empty<Bytes>>>,
}

impl RestClient {
    pub(crate) fn new(host: &'static str) -> Self {
        let roots = rustls::RootCertStore { roots: webpki_roots::TLS_SERVER_ROOTS.to_vec() };
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_safe_default_protocol_versions()
            .unwrap_or_else(|err| unreachable!("ring supports the default TLS versions: {err}"))
            .with_root_certificates(roots)
            .with_no_client_auth();
        Self { host, tls: TlsConnector::from(Arc::new(config)), sender: None }
    }

    /// The body of a 200 answer to `method path`. A transport failure drops the connection (the next call reconnects);
    /// a status other than the transient ones is a contract error.
    pub(crate) async fn call(&mut self, method: Method, path: &str) -> Result<Bytes, FeedError> {
        let result = tokio::time::timeout(TIMEOUT, self.call_once(method, path)).await;
        let result = result.unwrap_or_else(|_| Err(FeedError::Transient(format!("{} {path}: no answer within {TIMEOUT:?}", self.host))));
        if result.is_err() {
            self.sender = None;
        }
        result
    }

    async fn call_once(&mut self, method: Method, path: &str) -> Result<Bytes, FeedError> {
        let transient = |err: &dyn std::fmt::Display| FeedError::Transient(format!("{} {path}: {err}", self.host));
        if self.sender.as_ref().is_none_or(SendRequest::is_closed) {
            self.sender = Some(self.connect().await?);
        }
        let sender = self.sender.as_mut().unwrap_or_else(|| unreachable!("connected above"));
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, self.host)
            .header(header::USER_AGENT, "orderbook-server-local-marks")
            .header(header::CONTENT_LENGTH, 0)
            .body(Empty::new())
            .unwrap_or_else(|err| unreachable!("a request of a constant host and path: {err}"));
        let response = sender.send_request(request).await.map_err(|err| transient(&err))?;
        let status = response.status();
        let body = response.into_body().collect().await.map_err(|err| transient(&err))?.to_bytes();
        match status {
            StatusCode::OK => Ok(body),
            status if TRANSIENT_STATUS.contains(&status.as_u16()) => Err(FeedError::Transient(format!("{} {path}: HTTP {status}", self.host))),
            status => Err(FeedError::Contract(format!("{} {path}: HTTP {status}: {}", self.host, String::from_utf8_lossy(&body[..body.len().min(300)])))),
        }
    }

    async fn connect(&self) -> Result<SendRequest<Empty<Bytes>>, FeedError> {
        let transient = |err: &dyn std::fmt::Display| FeedError::Transient(format!("{}: {err}", self.host));
        let tcp = TcpStream::connect((self.host, 443)).await.map_err(|err| transient(&err))?;
        let name = ServerName::try_from(self.host).unwrap_or_else(|err| unreachable!("a constant host name: {err}"));
        // a certificate the roots refuse is no transient failure
        let tls = self.tls.connect(name, tcp).await.map_err(|err| match err.kind() {
            std::io::ErrorKind::InvalidData => FeedError::Contract(format!("{}: TLS: {err}", self.host)),
            _ => transient(&err),
        })?;
        let (sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(tls)).await.map_err(|err| transient(&err))?;
        tokio::spawn(async move {
            // ends with the connection; a failure surfaces on the next request
            let _ended = connection.await;
        });
        Ok(sender)
    }
}
