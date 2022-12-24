use containerd_snapshots::server;
use snapshotter::TarDevSnapshotter;
use std::{env, process, sync::Arc};
use tokio::net::UnixListener;
use tonic::transport::Server;

mod snapshotter;

#[tokio::main]
pub async fn main() {
    let argv: Vec<String> = env::args().collect();
    if argv.len() != 3 {
        eprintln!("Usage: {} <listen-socket-name> <data-root-path>", argv[0]);
        process::exit(1);
    }

    let incoming = {
        let uds = match UnixListener::bind(&argv[1]) {
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
        .add_service(server(Arc::new(TarDevSnapshotter::new(&argv[2]))))
        .serve_with_incoming(incoming)
        .await
    {
        eprintln!("serve_with_incoming failed: {:?}", e);
        process::exit(1);
    }
}
