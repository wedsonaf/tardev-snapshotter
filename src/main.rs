#![feature(type_alias_impl_trait)]

use containerd_snapshots::server;
use snapshotter::TarDevSnapshotter;
use std::{env, path::Path, process, sync::Arc};
use tokio::net::UnixListener;
use tonic::transport::Server;

mod snapshotter;

#[tokio::main]
pub async fn main() {
    let argv: Vec<String> = env::args().collect();
    if argv.len() != 3 {
        eprintln!("Usage: {} <data-root-path> [listen-socket-name]", argv[0]);
        process::exit(1);
    }

    // TODO: Add support for getting the listening socket

    // TODO: Check that the directory is accessible.

    let incoming = {
        let uds = match UnixListener::bind(&argv[2]) {
            Ok(l) => l,
            Err(e) => {
                eprintln!("UnixListener::bind failed: {:?}", e);
                process::exit(1);
            }
        };

        async_stream::stream! {
            loop {
                let item = uds.accept().await.map(|p| p.0);
                yield item;
            }
        }
    };

    if let Err(e) = Server::builder()
        .add_service(server(Arc::new(TarDevSnapshotter::new(Path::new(
            &argv[1],
        )))))
        .serve_with_incoming(incoming)
        .await
    {
        eprintln!("serve_with_incoming failed: {:?}", e);
        process::exit(1);
    }
}
