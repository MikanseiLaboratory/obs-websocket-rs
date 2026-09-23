#![cfg(feature = "rustls")]

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use obs_websocket_core::SessionConfig;
use obs_websocket_io::Connection;
use obs_websocket_mock::{MockConfig, MockObs};
use obs_websocket_tokio::{TokioTimer, TokioTransport};
use rcgen::{CertificateParams, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::accept_hdr_async;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;

#[allow(clippy::result_large_err)]
fn select_subprotocol(
    request: &Request,
    mut response: Response,
) -> Result<Response, ErrorResponse> {
    if request.headers().get("Sec-WebSocket-Protocol").is_some() {
        response.headers_mut().insert(
            "Sec-WebSocket-Protocol",
            HeaderValue::from_static("obswebsocket.json"),
        );
    }
    Ok(response)
}

#[tokio::test]
async fn connects_over_wss_with_a_test_certificate() {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::V4(Ipv4Addr::LOCALHOST)));
    let key_pair = KeyPair::generate().unwrap();
    let issued = params.self_signed(&key_pair).unwrap();
    let cert = CertificateDer::from(issued.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der()));

    let server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key)
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(server_config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = MockObs::spawn(MockConfig::default()).await.unwrap();
    let accept_server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let tls = acceptor.accept(tcp).await.unwrap();
        let socket = accept_hdr_async(tls, select_subprotocol).await.unwrap();
        server.serve(socket).await;
    });

    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    let client_config = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let transport = TokioTransport::connect_with_rustls(
        &format!("wss://127.0.0.1:{port}"),
        Duration::from_secs(2),
        Arc::new(client_config),
    )
    .await
    .expect("wss");
    let mut connection = Connection::connect(
        transport,
        SessionConfig::default(),
        2_000,
        &TokioTimer::new(),
    )
    .await
    .expect("identify");
    let version = connection
        .request(
            &obs_websocket_core::requests::GetVersion::new(),
            &TokioTimer::new(),
        )
        .await
        .unwrap();
    assert_eq!(version.obs_web_socket_version, "5.7.4");
    accept_server.abort();
}
