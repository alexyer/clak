use std::{cmp::Reverse, collections::HashMap};

use serde::{Deserialize, Serialize};

use crate::{
    member::{Member, MemberState},
    Address,
};

#[derive(Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Clone, PartialEq))]
pub enum Message<A: Address> {
    JoinSuccess,
    Ping(OverheardGossips<A>),
    PingReq(A, OverheardGossips<A>),
    Ack(A, OverheardGossips<A>),
}

pub type OverheardGossips<A> = Vec<Gossip<A>>;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Gossip<A: Address> {
    pub member: Member<A>,
    pub state: MemberState,
    pub lamport: usize,
}

impl<A> Gossip<A>
where
    A: Address,
{
    pub fn new(member: Member<A>, state: MemberState) -> Self {
        Self {
            member,
            state,
            lamport: 0,
        }
    }
}

impl<A> From<Reverse<Gossip<A>>> for Gossip<A>
where
    A: Address,
{
    fn from(value: Reverse<Gossip<A>>) -> Self {
        value.0
    }
}

#[derive(Default)]
pub(crate) struct Gossips<A: Address> {
    gossips: HashMap<A, (Gossip<A>, usize)>,
    max_age: usize,
}

impl<A> Gossips<A>
where
    A: Address,
{
    pub fn new(max_age: usize) -> Self {
        Self {
            max_age,
            gossips: Default::default(),
        }
    }
    pub fn push(&mut self, gossip: Gossip<A>) {
        let (entry, age) = self
            .gossips
            .entry(gossip.member.address().clone())
            .or_insert_with(|| (gossip.clone(), 0));

        if gossip.lamport > entry.lamport && *age < self.max_age {
            *entry = gossip;
            *age += 1;
        }
    }

    pub fn take(&mut self, n: usize) -> Vec<Gossip<A>> {
        let gossips = self
            .gossips
            .iter_mut()
            .take(n)
            .map(|(_, (gossip, age))| {
                gossip.lamport += 1;
                *age += 1;

                gossip.clone()
            })
            .collect();

        // FIXME(alexyer): suboptimal
        self.purge();

        gossips
    }

    fn purge(&mut self) {
        self.gossips.retain(|_, (_, age)| *age < self.max_age);
    }
}
