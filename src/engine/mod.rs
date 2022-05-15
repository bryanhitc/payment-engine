#[cfg_attr(feature = "serial", path = "serial.rs")]
#[cfg_attr(feature = "stream", path = "stream.rs")]
pub(crate) mod engine_impl;

pub type Engine = engine_impl::Engine;

use log::{debug, error};
use std::{collections::HashMap, fmt::Display};

use crate::{Client, ClientId, ClientSnapshot, Transaction, TransactionId, TransactionType};

// Manages client(s) and is used by TransactionProcessor.
//
// TODO (PERF + ENHANCEMENT + MAINTANABILITY): Ideally, the
// disputes and basic_transaction (deposits + withdrawals) in Client should
// be moved into the implementation of this trait. This would allow us to have
// a single BTreeMap/HashMap for disputes, and a single BTreeSet/HashSet
// for *all* clients, for example, while also supporting client-partitioned data
// structures. TransactionProcessor would then insert disputes, remove disputes,
// and insert transactions via this interface. I think I've already shown that I
// can generalize via traits, though, so I'm not going to add more noise + spend
// the time to do that.
trait ClientManager {
    fn get_or_insert_client_mut(&mut self, client_id: ClientId) -> &mut Client;
}

#[derive(Debug, Default)]
pub struct MultiClientManager {
    clients: HashMap<ClientId, Client>,
}

impl ClientManager for MultiClientManager {
    fn get_or_insert_client_mut(&mut self, client_id: ClientId) -> &mut Client {
        self.clients
            .entry(client_id)
            .or_insert_with(|| Client::new(client_id))
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub enum TransactionProcessError {
    ClientLocked(ClientId, TransactionId),
    InsufficientFunds(ClientId, TransactionId),
    InvalidDisputeNotFound(ClientId, TransactionId),
    InvalidDisputeDuplicate(ClientId, TransactionId),
    InvalidResolveNotFound(ClientId, TransactionId),
    InvalidResolveNotDisputed(ClientId, TransactionId),
    InvalidChargeBackNotFound(ClientId, TransactionId),
    InvalidChargeBackNotDisputed(ClientId, TransactionId),
    Unknown,
}

impl Display for TransactionProcessError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("[Client] Transaction failed: {:?}", self))
    }
}

// Contains the core business logic for processing transactions
#[derive(Debug, Default)]
struct TransactionProcessor<C>
where
    C: ClientManager,
{
    client_manager: C,
}

impl<C> TransactionProcessor<C>
where
    C: ClientManager,
{
    fn process(&mut self, transaction: Transaction) -> Result<(), TransactionProcessError> {
        debug!(
            "[Client {}] Processing transaction: {:?}",
            transaction.client_id, transaction
        );

        let id = transaction.id;
        let client = self
            .client_manager
            .get_or_insert_client_mut(transaction.client_id);

        if client.is_locked {
            // In a real system, we probably don't want to drop a transaction
            // if the account is locked, but rather keep it in a separate queue.
            // I'm just going to drop it for this coding exercise, though :)
            return Err(TransactionProcessError::ClientLocked(client.id, id));
        }

        match transaction.action {
            TransactionType::Deposit => {
                // As mentioned elsewhere, if csv + serde weren't giving me problems,
                // I would've included the amount in the deposit and withdrawal variants
                // so we don't need .unwrap()...
                //
                // This invariant is *currently* upheld throughout the project, though,
                // so this is safe.
                let amount = transaction.amount.unwrap();

                client.available += amount;
                client
                    .basic_transactions
                    .insert(transaction.id, transaction);

                Ok(())
            }
            TransactionType::Withdrawal => {
                let amount = transaction.amount.unwrap();

                (client.available >= amount)
                    .then(|| {
                        client.available -= amount;
                        client
                            .basic_transactions
                            .insert(transaction.id, transaction);
                    })
                    .ok_or(TransactionProcessError::InsufficientFunds(client.id, id))
            }
            TransactionType::Dispute => {
                let basic_transaction = client.basic_transactions.get(&id).ok_or(
                    TransactionProcessError::InvalidDisputeNotFound(client.id, id),
                )?;

                client
                    .disputes
                    .insert(id)
                    .then(|| {
                        let amount = basic_transaction.amount.unwrap();

                        // Not sure if charging back a withdrawal (sending money back) makes sense...
                        // TODO (ENHANCEMENT + MAINTAINABILITY): We should have a single variant
                        // for this + simply change amount's sign.
                        match basic_transaction.action {
                            TransactionType::Deposit => {
                                client.available -= amount;
                                client.held += amount;
                            }
                            TransactionType::Withdrawal => {
                                client.available += amount;
                                client.held -= amount;
                            }
                            _ => unreachable!("invariant violated"),
                        }
                    })
                    .ok_or(TransactionProcessError::InvalidDisputeDuplicate(
                        client.id, id,
                    ))
            }
            TransactionType::Resolve => {
                let basic_transaction = client.basic_transactions.get(&id).ok_or(
                    TransactionProcessError::InvalidResolveNotFound(client.id, id),
                )?;

                client
                    .disputes
                    .remove(&id)
                    .then(|| {
                        let amount = basic_transaction.amount.unwrap();

                        // Not sure if charging back a withdrawal (sending money back) makes sense...
                        // TODO (ENHANCEMENT + MAINTAINABILITY): We should have a single variant
                        // for this + simply change amount's sign.
                        match basic_transaction.action {
                            TransactionType::Deposit => {
                                client.held -= amount;
                                client.available += amount;
                            }
                            TransactionType::Withdrawal => {
                                client.held += amount;
                                client.available -= amount;
                            }
                            _ => unreachable!("invariant violated"),
                        }
                    })
                    .ok_or(TransactionProcessError::InvalidResolveNotDisputed(
                        client.id, id,
                    ))
            }
            TransactionType::Chargeback => {
                let basic_transaction = client.basic_transactions.get(&id).ok_or(
                    TransactionProcessError::InvalidChargeBackNotFound(client.id, id),
                )?;

                client
                    .disputes
                    .remove(&id)
                    .then(|| {
                        let amount = basic_transaction.amount.unwrap();

                        // Should we lock the account if the user charge backs a withdrawal (sends money back)??
                        client.is_locked = true;

                        // Not sure if charging back a withdrawal (sending money back) makes sense...
                        // TODO (ENHANCEMENT + MAINTAINABILITY): We should have a single variant
                        // for this + simply change amount's sign.
                        match basic_transaction.action {
                            TransactionType::Deposit => {
                                client.held -= amount;
                            }
                            TransactionType::Withdrawal => {
                                client.held += amount;
                            }
                            _ => unreachable!("invariant violated"),
                        }
                    })
                    .ok_or(TransactionProcessError::InvalidChargeBackNotDisputed(
                        client.id, id,
                    ))
            }
        }
    }
}

// Represents a engine for processing all payments in a system
pub trait PaymentEngine {
    type ProcessError;
    type SnapshotError;

    fn process(&mut self, transaction: Transaction) -> Result<(), Self::ProcessError>;
    fn finalize(self) -> Vec<Result<ClientSnapshot, Self::SnapshotError>>;
}

#[cfg(test)]
mod processor_tests {
    use super::*;
    use crate::parse::Amount;

    #[test]
    pub fn can_not_double_resolve() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        let resolve = Transaction::new(2, 1, TransactionType::Resolve, None);

        assert_eq!(Ok(()), processor.process(resolve.clone()));

        assert_eq!(
            Err(TransactionProcessError::InvalidResolveNotDisputed(1, 2)),
            processor.process(resolve)
        );
    }

    #[test]
    pub fn can_not_resolve_without_dispute() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        let resolve = Transaction::new(2, 1, TransactionType::Resolve, None);

        assert_eq!(
            Err(TransactionProcessError::InvalidResolveNotFound(1, 2)),
            processor.process(resolve.clone())
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        assert_eq!(
            Err(TransactionProcessError::InvalidResolveNotDisputed(1, 2)),
            processor.process(resolve.clone())
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        assert_eq!(Ok(()), processor.process(resolve.clone()));

        assert_eq!(
            Err(TransactionProcessError::InvalidResolveNotDisputed(1, 2)),
            processor.process(resolve)
        );
    }

    #[test]
    pub fn can_not_charge_back_without_dispute() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        let chargeback = Transaction::new(2, 1, TransactionType::Chargeback, None);

        assert_eq!(
            Err(TransactionProcessError::InvalidChargeBackNotFound(1, 2)),
            processor.process(chargeback.clone())
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        assert_eq!(
            Err(TransactionProcessError::InvalidChargeBackNotDisputed(1, 2)),
            processor.process(chargeback.clone())
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        assert_eq!(Ok(()), processor.process(chargeback.clone()));

        assert_eq!(
            Err(TransactionProcessError::ClientLocked(1, 2)),
            processor.process(chargeback)
        );
    }

    #[test]
    pub fn duplicate_dispute_does_not_affect_client_balances() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                1,
                1,
                TransactionType::Deposit,
                Amount::new(5.0).ok()
            ))
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(8.0).unwrap());
            assert_eq!(client.held, Amount::from(0));
        }

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(5.0).unwrap());
            assert_eq!(client.held, Amount::new(3.0).unwrap());
        }

        assert_eq!(
            Err(TransactionProcessError::InvalidDisputeDuplicate(1, 2)),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        let client = processor.client_manager.get_or_insert_client_mut(1);

        assert_eq!(client.available, Amount::new(5.0).unwrap());
        assert_eq!(client.held, Amount::new(3.0).unwrap());
    }

    #[test]
    pub fn charging_back_locks_client() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        assert!(
            !processor
                .client_manager
                .get_or_insert_client_mut(1)
                .is_locked
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Chargeback, None))
        );

        assert!(
            processor
                .client_manager
                .get_or_insert_client_mut(1)
                .is_locked
        );
    }

    #[test]
    pub fn deposit_charge_back_releases_held_funds() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                1,
                1,
                TransactionType::Deposit,
                Amount::new(1.5).ok()
            ))
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(4.5).unwrap());
            assert_eq!(client.held, Amount::from(0));
        }

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(1.5).unwrap());
            assert_eq!(client.held, Amount::new(3.0).unwrap());
        }

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Chargeback, None))
        );

        let client = processor.client_manager.get_or_insert_client_mut(1);

        assert_eq!(client.available, Amount::new(1.5).unwrap());
        assert_eq!(client.held, Amount::from(0),);
    }

    #[test]
    pub fn can_not_process_locked_client() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        let client = processor.client_manager.get_or_insert_client_mut(1);
        client.is_locked = true;

        let mut transaction =
            Transaction::new(2, 1, TransactionType::Deposit, Amount::new(3.0).ok());

        let result = processor.process(transaction.clone());

        assert_eq!(Err(TransactionProcessError::ClientLocked(1, 2)), result);

        transaction.action = TransactionType::Withdrawal;
        let result = processor.process(transaction.clone());

        assert_eq!(Err(TransactionProcessError::ClientLocked(1, 2)), result);

        transaction.action = TransactionType::Dispute;
        let result = processor.process(transaction.clone());

        assert_eq!(Err(TransactionProcessError::ClientLocked(1, 2)), result);

        transaction.action = TransactionType::Resolve;
        let result = processor.process(transaction.clone());

        assert_eq!(Err(TransactionProcessError::ClientLocked(1, 2)), result);

        transaction.action = TransactionType::Chargeback;
        let result = processor.process(transaction);

        assert_eq!(Err(TransactionProcessError::ClientLocked(1, 2)), result);
    }

    #[test]
    pub fn can_not_dispute_other_clients_transactions() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        processor.client_manager.get_or_insert_client_mut(1);
        processor.client_manager.get_or_insert_client_mut(2);

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        assert_eq!(
            Err(TransactionProcessError::InvalidDisputeNotFound(2, 2)),
            processor.process(Transaction::new(2, 2, TransactionType::Dispute, None))
        );
    }

    #[test]
    pub fn client_lock_does_not_impact_other_clients() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        processor.client_manager.get_or_insert_client_mut(1);
        processor.client_manager.get_or_insert_client_mut(2);

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            ))
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None))
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Chargeback, None))
        );

        assert!(
            processor
                .client_manager
                .get_or_insert_client_mut(1)
                .is_locked
        );

        assert!(
            !processor
                .client_manager
                .get_or_insert_client_mut(2)
                .is_locked
        );
    }

    #[test]
    fn dispute_and_resolve_is_noop() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                1,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            )),
        );

        assert_eq!(
            processor
                .client_manager
                .get_or_insert_client_mut(1)
                .available,
            Amount::new(3.0).unwrap(),
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(1, 1, TransactionType::Dispute, None)),
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(1, 1, TransactionType::Resolve, None)),
        );

        assert_eq!(
            processor
                .client_manager
                .get_or_insert_client_mut(1)
                .available,
            Amount::new(3.0).unwrap(),
        );
    }

    #[test]
    fn withdraw_holds_negative_funds_and_credits_available() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            )),
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Withdrawal,
                Amount::new(1.0).ok()
            )),
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(2.0).unwrap());
            assert_eq!(client.held, Amount::from(0));
        }

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None)),
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(3.0).unwrap());
            assert_eq!(client.held, Amount::new(-1.0).unwrap());
        }

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Resolve, None)),
        );

        let client = processor.client_manager.get_or_insert_client_mut(1);

        assert_eq!(client.available, Amount::new(2.0).unwrap(),);
        assert_eq!(client.held, Amount::from(0),);
    }

    #[test]
    fn withdraw_charge_back_releases_held_funds() {
        let mut processor = TransactionProcessor::<MultiClientManager>::default();

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Deposit,
                Amount::new(3.0).ok()
            )),
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(
                2,
                1,
                TransactionType::Withdrawal,
                Amount::new(1.0).ok()
            )),
        );

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Dispute, None)),
        );

        {
            let client = processor.client_manager.get_or_insert_client_mut(1);

            assert_eq!(client.available, Amount::new(3.0).unwrap());
            assert_eq!(client.held, Amount::new(-1.0).unwrap());
        }

        assert_eq!(
            Ok(()),
            processor.process(Transaction::new(2, 1, TransactionType::Chargeback, None)),
        );

        let client = processor.client_manager.get_or_insert_client_mut(1);

        assert!(client.is_locked);
        assert_eq!(client.available, Amount::new(3.0).unwrap(),);
        assert_eq!(client.held, Amount::from(0),);
    }
}
