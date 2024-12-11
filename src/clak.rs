use rand::seq::IteratorRandom;
use std::{
    collections::HashMap,
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{
    oneshot::{channel, Receiver, Sender},
    Mutex,
};

use crate::{
    member::{Member, MemberState},
    message::{Gossip, Gossips, Message, OverheardGossips},
    transport::Transport,
    Address,
};

pub struct ClakConfig<A: Address> {
    pub address: A,
    pub protocol_period: Duration,
    pub protocol_timeout: Duration,
    pub ping_req_group_size: usize,
    pub gossip_max_age: usize,
    pub gossip_overheard_size: usize,
}

pub struct Clak<T: Transport> {
    members: Arc<Mutex<HashMap<T::TransportAddress, Member<T::TransportAddress>>>>,
    direct_pings: Arc<
        Mutex<HashMap<T::TransportAddress, (Sender<T::TransportAddress>, T::TransportAddress)>>,
    >,
    indirect_pings: Arc<Mutex<HashMap<T::TransportAddress, Sender<T::TransportAddress>>>>,
    gossips: Arc<Mutex<Gossips<T::TransportAddress>>>,
    transport: T,

    protocol_period: Duration,
    protocol_timeout: Duration,
    ping_req_group_size: usize,
    gossip_overheard_size: usize,
}

impl<T> Clak<T>
where
    T: Transport,
    T::TransportAddress: Clone + Eq + Hash + Debug,
    T::Error: Display + Debug,
{
    pub async fn new(config: ClakConfig<T::TransportAddress>) -> Result<Self, T::Error> {
        Ok(Self {
            members: Default::default(),
            transport: T::bind(config.address).await?,
            protocol_period: config.protocol_period,
            protocol_timeout: config.protocol_timeout,
            ping_req_group_size: config.ping_req_group_size,
            direct_pings: Default::default(),
            indirect_pings: Default::default(),
            gossip_overheard_size: config.gossip_overheard_size,
            gossips: Arc::new(Mutex::new(Gossips::new(config.gossip_max_age))),
        })
    }

    pub async fn run(&mut self) {
        tokio::join!(self.listen(), self.detect());
    }

    pub async fn detect(&self) {
        loop {
            tokio::time::sleep(self.protocol_period).await;
            let Some(ping_address) = self
                .members
                .lock()
                .await
                .values()
                .choose(&mut rand::thread_rng())
                .map(|m| m.address().clone())
            else {
                continue;
            };

            let state = self
                .direct_probe(&ping_address, self.transport.address())
                .await;

            match state {
                MemberState::Alive => {
                    log::debug!(target: "clak", "{ping_address} alive");
                    continue;
                }
                MemberState::Suspicious | MemberState::Dead => (),
            }

            let ping_req_addresses: Vec<_> = self
                .members
                .lock()
                .await
                .values()
                .choose_multiple(&mut rand::thread_rng(), self.ping_req_group_size)
                .into_iter()
                .map(|m| m.address().clone())
                .filter(|a| a != &ping_address)
                .collect();

            let state = self
                .indirect_probe(&ping_address, ping_req_addresses.as_slice())
                .await;

            match state {
                MemberState::Alive => {
                    log::debug!(target: "clak", "{ping_address} alive");
                    continue;
                }
                MemberState::Suspicious | MemberState::Dead => {
                    self.dead_member(ping_address).await;
                }
            }
        }
    }

    async fn direct_probe(
        &self,
        ping_address: &T::TransportAddress,
        response_address: T::TransportAddress,
    ) -> MemberState {
        let address = ping_address.clone();

        let (tx, rx) = channel();

        self.direct_pings
            .lock()
            .await
            .insert(ping_address.clone(), (tx, response_address));

        let overheard_gossips = self.get_gossips().await;

        match self
            .transport
            .send(&Message::Ping(overheard_gossips), &address)
            .await
        {
            Ok(_) => log::debug!(target: "clak", "ping {address}"),
            Err(e) => log::error!(target: "clak", "❗️ failed to ping {address}: {e}"),
        };

        self.wait_for_ack(address, rx).await
    }

    async fn indirect_probe(
        &self,
        ping_address: &T::TransportAddress,
        ping_req_addresses: &[T::TransportAddress],
    ) -> MemberState {
        if ping_req_addresses.is_empty() {
            return MemberState::Suspicious;
        }

        let address = ping_address.clone();

        let (tx, rx) = channel();

        self.indirect_pings
            .lock()
            .await
            .insert(ping_address.clone(), tx);

        for address in ping_req_addresses {
            let overheard_gossips = self.get_gossips().await;

            match self
                .transport
                .send(
                    &Message::PingReq(ping_address.clone(), overheard_gossips),
                    address,
                )
                .await
            {
                Ok(_) => log::debug!(target: "clak", "ping {address}"),
                Err(e) => log::error!(target: "clak", "❗️ failed to ping {address}: {e}"),
            };
        }

        self.wait_for_ack(address, rx).await
    }

    async fn get_gossips(&self) -> Vec<Gossip<T::TransportAddress>> {
        let mut gossips = self.gossips.lock().await;
        gossips.take(self.gossip_overheard_size)
    }

    async fn wait_for_ack(
        &self,
        address: T::TransportAddress,
        rx: Receiver<<T as Transport>::TransportAddress>,
    ) -> MemberState {
        tokio::select! {
            rx = rx => match rx {
                Ok(recieved_address) => {
                    if recieved_address != address {
                        log::error!(target: "clak", "invalid ack recived {recieved_address}. expected: {address}");
                        MemberState::Suspicious
                    } else {
                        MemberState::Alive
                    }
                }
                Err(e) => {
                    log::error!(target: "clak", "error while recieving ack from {address}: {e}");
                    MemberState::Suspicious
                }
            },
            _ = tokio::time::sleep(self.protocol_timeout) => {
                log::debug!(target: "clack", "sus");
                MemberState::Suspicious
            }
        }
    }

    pub async fn listen(&self) {
        loop {
            if let Some((msg, address)) = self.transport.next().await {
                self.process(msg, address).await;
            } else {
                continue;
            }
        }
    }

    pub async fn join(&mut self, target: &<T as Transport>::TransportAddress) {
        self.transport
            .send(
                &Message::Ack(self.transport.address(), self.get_gossips().await),
                target,
            )
            .await
            .expect("could not join");

        self.run().await;
    }

    async fn process(
        &self,
        msg: Message<T::TransportAddress>,
        from: <T as Transport>::TransportAddress,
    ) {
        if !self.members.lock().await.contains_key(&from) {
            if let Err(e) = self.handle_join(from.clone()).await {
                log::error!(target: "clak", "error while handling join: {e}")
            }
        }

        let res = match msg {
            Message::JoinSuccess => self.handle_join_success(from).await,
            Message::Ping(gossips) => self.handle_ping(from, gossips).await,
            Message::PingReq(address, gossips) => {
                self.handle_ping_req(address, from, gossips).await
            }
            Message::Ack(address, gossips) => self.handle_ack(address, gossips).await,
        };

        match res {
            Ok(_) => {}
            Err(e) => log::error!(target: "clak", "error while handling msg: {e}"),
        }
    }

    async fn process_gossips(&self, overheard_gossips: OverheardGossips<T::TransportAddress>) {
        for gossip in overheard_gossips.into_iter() {
            let address = gossip.member.address().clone();
            if address != self.transport.address() {
                let is_known = self.members.lock().await.contains_key(&address);
                match gossip.state {
                    MemberState::Alive => {
                        if !is_known {
                            self.gossips.lock().await.push(gossip);
                            self.new_member(address).await;
                        }
                    }
                    MemberState::Dead => {
                        if is_known {
                            self.gossips.lock().await.push(gossip);
                            self.dead_member(address).await
                        }
                    }
                    MemberState::Suspicious => (),
                };
            }
        }
    }

    async fn handle_join(
        &self,
        address: <T as Transport>::TransportAddress,
    ) -> Result<(), <T as Transport>::Error> {
        match self.transport.send(&Message::JoinSuccess, &address).await {
            Ok(_) => {
                self.new_member(address.clone()).await;

                Ok(())
            }
            Err(e) => {
                log::error!(target: "clak", "❗️ failed to send join request: {e}");

                Err(e)
            }
        }
    }

    async fn new_member(&self, address: <T as Transport>::TransportAddress) {
        log::info!(target: "clak", "🎉 member joined: {address}");

        self.members
            .lock()
            .await
            .insert(address.clone(), Member::new(address.clone()));

        self.gossips
            .lock()
            .await
            .push(Gossip::new(Member::new(address), MemberState::Alive));
    }

    async fn dead_member(&self, address: <T as Transport>::TransportAddress) {
        if self.members.lock().await.contains_key(&address) {
            log::info!(target: "clak", "❌ {address} disconnected");
        }

        self.members.lock().await.remove(&address);
        self.gossips
            .lock()
            .await
            .push(Gossip::new(Member::new(address), MemberState::Dead));
    }

    async fn handle_join_success(
        &self,
        address: <T as Transport>::TransportAddress,
    ) -> Result<(), <T as Transport>::Error> {
        log::info!(target: "clak", "🎉 joined to {address}");
        self.members
            .lock()
            .await
            .insert(address.clone(), Member::new(address));

        Ok(())
    }

    async fn handle_ping(
        &self,
        to: <T as Transport>::TransportAddress,
        gossips: OverheardGossips<T::TransportAddress>,
    ) -> Result<(), <T as Transport>::Error> {
        log::debug!(target: "clak", "ping from {to}");

        self.process_gossips(gossips).await;
        let overheard_gossips = self.get_gossips().await;

        self.transport
            .send(
                &Message::Ack(self.transport.address(), overheard_gossips),
                &to,
            )
            .await?;

        Ok(())
    }

    async fn handle_ping_req(
        &self,
        ping_address: T::TransportAddress,
        response_address: T::TransportAddress,
        overheard_gossips: OverheardGossips<T::TransportAddress>,
    ) -> Result<(), <T as Transport>::Error> {
        log::debug!(target: "clak", "ping request {response_address} -> {ping_address}");

        self.process_gossips(overheard_gossips).await;

        self.direct_probe(&ping_address, response_address).await;

        Ok(())
    }

    async fn handle_ack(
        &self,
        address: <T as Transport>::TransportAddress,
        overheard_gossips: OverheardGossips<T::TransportAddress>,
    ) -> Result<(), <T as Transport>::Error> {
        self.process_gossips(overheard_gossips).await;

        if let Some((tx, response_address)) =
            self.direct_pings.clone().lock().await.remove(&address)
        {
            // notify another member who requested this ping.
            if self.transport.address() != response_address {
                self.transport
                    .send(
                        &Message::Ack(address.clone(), self.get_gossips().await),
                        &response_address,
                    )
                    .await?;
            }

            if tx.send(address).is_err() {
                // channel closed
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use super::*;

    #[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
    struct TestAddress(pub String);

    impl Display for TestAddress {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Display::fmt(&self.0, f)
        }
    }

    impl Address for TestAddress {}

    #[derive(Debug, Default)]
    struct TestTransportInner {
        incoming_messages: VecDeque<(Message<TestAddress>, TestAddress)>,
        outcoming_messages: VecDeque<(Message<TestAddress>, TestAddress)>,
    }

    #[derive(Debug, Default, Clone)]
    struct TestTransport {
        address: TestAddress,
        inner: Arc<Mutex<TestTransportInner>>,
    }

    impl Transport for TestTransport {
        type TransportAddress = TestAddress;

        type Error = String;

        async fn bind(address: Self::TransportAddress) -> Result<Self, Self::Error> {
            Ok(TestTransport {
                address,
                ..Default::default()
            })
        }

        async fn next(&self) -> Option<(Message<Self::TransportAddress>, Self::TransportAddress)> {
            self.inner.lock().await.incoming_messages.pop_front()
        }

        async fn send(
            &self,
            msg: &Message<Self::TransportAddress>,
            target: &Self::TransportAddress,
        ) -> Result<usize, Self::Error> {
            self.inner
                .lock()
                .await
                .outcoming_messages
                .push_back((msg.clone(), target.clone()));

            Ok(1)
        }

        fn address(&self) -> Self::TransportAddress {
            self.address.clone()
        }
    }

    #[tokio::test]
    async fn test_join() {
        let mut node: Clak<TestTransport> = Clak::new(ClakConfig {
            address: TestAddress("node1".into()),
            protocol_period: Duration::from_secs(1),
            protocol_timeout: Duration::from_millis(300),
            ping_req_group_size: 3,
            gossip_max_age: 5,
            gossip_overheard_size: 5,
        })
        .await
        .unwrap();

        let members = node.members.clone();
        let transport = node.transport.clone();

        tokio::spawn(async move {
            node.join(&TestAddress("node2".into())).await;
        });

        transport
            .inner
            .lock()
            .await
            .incoming_messages
            .push_back((Message::JoinSuccess, TestAddress("node2".into())));

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(1)).await;

        assert_eq!(
            transport.inner.lock().await.outcoming_messages[0],
            (
                Message::Ack(TestAddress("node1".into()), vec![]),
                TestAddress("node2".into()),
            ),
        );

        assert!(members
            .lock()
            .await
            .get(&TestAddress("node2".into()))
            .is_some());
    }

    #[tokio::test]
    async fn test_ping_add_member() {
        let mut node: Clak<TestTransport> = Clak::new(ClakConfig {
            address: TestAddress("node1".into()),
            protocol_period: Duration::from_secs(1),
            protocol_timeout: Duration::from_millis(300),
            ping_req_group_size: 3,
            gossip_max_age: 5,
            gossip_overheard_size: 5,
        })
        .await
        .unwrap();

        let members = node.members.clone();
        let transport = node.transport.clone();

        tokio::spawn(async move {
            node.run().await;
        });

        transport
            .inner
            .lock()
            .await
            .incoming_messages
            .push_back((Message::Ping(Vec::new()), TestAddress("node2".into())));

        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(1)).await;

        assert!(members
            .lock()
            .await
            .contains_key(&TestAddress("node2".into())));
    }
}
