use crate::Address;

pub(crate) struct Member<A: Address> {
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

pub(crate) enum MemberState {
    Alive,
    Suspicious,
}
