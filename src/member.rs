use serde::{Deserialize, Serialize};

use crate::Address;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member<A: Address> {
    address: A,
}

impl<A> Member<A>
where
    A: Address,
{
    pub fn new(address: A) -> Self {
        Self { address }
    }

    pub fn address(&self) -> &A {
        &self.address
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MemberState {
    Alive,
    Suspicious,
    Dead,
}
