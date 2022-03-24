pub mod engine;
pub mod parse;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use parse::Amount;
use serde::{Deserialize, Serialize};

pub type ClientId = u16;
type TransactionId = u32;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Debug, Deserialize, PartialEq, Serialize)]
pub struct Transaction {
    #[serde(rename = "tx")]
    pub id: TransactionId,
    #[serde(skip)]
    pub chrono_order: usize,
    #[serde(rename = "client")]
    pub client_id: ClientId,
    #[serde(rename = "type")]
    pub action: TransactionType,
    // Ideally, `Amount` would be in `TransactionType` only for
    // `Deposit` and `Withdrawal` variants, but csv + serde are
    // not playing nicely and I don't want to implement a
    // custom deserializer/serializer for this struct.
    //
    // Paying the cost of branching when we know whether this
    // is Some(T) or None based on the type is unfortunate.
    // It *should* be enforced via the type system.
    pub amount: Option<Amount>,
}

#[derive(Debug, Serialize)]
pub struct ClientSnapshot {
    client: ClientId,
    available: Amount,
    held: Amount,
    total: Amount,
    locked: bool,
}

impl From<&Client> for ClientSnapshot {
    fn from(client: &Client) -> Self {
        ClientSnapshot {
            client: client.id,
            available: client.available,
            held: client.held,
            total: client.available + client.held,
            locked: client.is_locked,
        }
    }
}

#[derive(Debug)]
struct Client {
    id: ClientId,
    available: Amount,
    held: Amount,
    is_locked: bool,
    // Should be something like an LRU distributed
    // cache in a real system. Cache miss => DB lookup.
    //
    // Again, would be nice if we could restrict this to only
    // deposit/withdrawal variants within the type system.
    //
    // Using BTreeMap + BTreeSet for less memory overhead
    basic_transactions: BTreeMap<TransactionId, Transaction>,
    disputes: BTreeSet<TransactionId>,
}

impl Client {
    pub fn new(id: ClientId) -> Self {
        Self {
            id,
            available: Amount::from(0),
            held: Amount::from(0),
            is_locked: false,
            basic_transactions: BTreeMap::new(),
            disputes: BTreeSet::new(),
        }
    }
}
