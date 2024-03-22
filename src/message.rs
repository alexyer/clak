use serde::{Deserialize, Serialize};

use crate::Address;

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub enum Message<A: Address> {
    JoinSuccess,
    Ping,
    PingReq(A),
    Ack(A),
}
