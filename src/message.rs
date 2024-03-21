use serde::{Deserialize, Serialize};

use crate::Address;

#[derive(Debug, Serialize, Deserialize)]
pub enum Message<A: Address> {
    JoinSuccess,
    Ping,
    PingReq(A),
    Ack(A),
}
