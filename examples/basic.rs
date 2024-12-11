use std::time::Duration;

use clak::{
    transport::{TcpAddress, UdpTransport},
    Clak, ClakConfig,
};

#[tokio::main]
async fn main() {
    env_logger::init();

    let host = std::env::var("HOST").expect("node host");
    let port = std::env::var("PORT")
        .expect("node host")
        .parse()
        .expect("valid port");

    let mut clak: Clak<UdpTransport> = Clak::new(ClakConfig {
        address: TcpAddress::new(host, port),
        protocol_period: Duration::from_secs(1),
        protocol_timeout: Duration::from_millis(300),
        ping_req_group_size: 3,
        gossip_max_age: 10,
        gossip_overheard_size: 10,
    })
    .await
    .unwrap();

    let join_host = std::env::var("JOIN_HOST");
    let join_port = std::env::var("JOIN_PORT");

    let join = join_host.and_then(|host| {
        let port = join_port?.parse().expect("valid join port");

        Ok(TcpAddress::new(host, port))
    });

    if let Ok(join) = join {
        clak.join(&join).await;
    } else {
        clak.run().await;
    }
}
