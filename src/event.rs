use crate::{
    member::{Member, MemberState},
    message::Gossip,
    Address,
};

#[derive(Debug)]
pub enum EventError {
    InvalidState(MemberState),
}

#[derive(Clone)]
pub enum Event<A: Address> {
    Alive(Member<A>),
    Dead(Member<A>),
}

impl<A> TryFrom<&Gossip<A>> for Event<A>
where
    A: Address,
{
    type Error = EventError;

    fn try_from(gossip: &Gossip<A>) -> Result<Self, Self::Error> {
        match gossip.state {
            MemberState::Alive => Ok(Self::Alive(gossip.member.clone())),
            MemberState::Dead => Ok(Self::Dead(gossip.member.clone())),
            MemberState::Suspicious => Err(EventError::InvalidState(MemberState::Suspicious)),
        }
    }
}
