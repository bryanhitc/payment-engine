use std::thread::JoinHandle;

use crossbeam::channel::{Receiver, SendError, Sender};
use log::{debug, info, warn};

use crate::*;

#[derive(Debug)]
pub struct ClientPaymentEngine {
    client: Client,
}

impl ClientPaymentEngine {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client: Client::new(client_id),
        }
    }

    pub fn generate_snapshot(&self) -> ClientSnapshot {
        ClientSnapshot::from(&self.client)
    }

    pub fn process(&mut self, transaction: Transaction) {
        if self.client.is_locked {
            // In a real system, we probably don't want to drop a transaction
            // if the account is locked, but rather keep it in a separate queue.
            // I'm just going to drop it for this coding exercise, though :)
            warn!(
                "[Client {}] account is locked; dropping transaction {:?}",
                self.client.id, transaction
            );

            return;
        }

        let id = transaction.id;

        match transaction.action {
            TransactionType::Deposit => {
                // As mentioned elsewhere, if csv + serde weren't giving me problems,
                // I would've included the amount in the deposit and withdrawal variants
                // so we don't need .unwrap()...
                //
                // This invariant is *currently* upheld throughout the project, though,
                // so this is safe.
                let amount = transaction.amount.unwrap();

                self.client.available += amount;
                self.client
                    .basic_transactions
                    .insert(transaction.id, transaction);
            }
            TransactionType::Withdrawal => {
                let amount = transaction.amount.unwrap();

                if self.client.available < amount {
                    // failing silently per PDF's instructions...
                    warn!(
                        "[Client {}] insufficient funds; dropping transaction: {:?}",
                        self.client.id, transaction
                    );
                    return;
                }

                self.client.available -= amount;
                self.client
                    .basic_transactions
                    .insert(transaction.id, transaction);
            }
            TransactionType::Dispute => {
                let basic_transaction = self.client.basic_transactions.get(&id);

                if basic_transaction.is_none() {
                    // failing silently per PDF's instructions
                    warn!(
                        "[Client {}] dispute does not reference valid deposit/withdrawal transaction; dropping transaction: {:?}",
                        self.client.id, transaction
                    );
                    return;
                }

                let basic_transaction = basic_transaction.unwrap();
                let is_duplicate_dispute = !self.client.disputes.insert(transaction.id);

                if is_duplicate_dispute {
                    warn!(
                        "[Client {}] received duplicate dispute; dropping transaction: {:?}",
                        self.client.id, transaction
                    );
                    return;
                }

                let amount = basic_transaction.amount.unwrap();

                self.client.available -= amount;
                self.client.held += amount;
            }
            TransactionType::Resolve => {
                let basic_transaction =
                    self.client
                        .basic_transactions
                        .get(&id)
                        .and_then(|basic_transaction| {
                            self.client.disputes.remove(&id).then(|| basic_transaction)
                        });

                if basic_transaction.is_none() {
                    // fail silently per PDF's instructions due to non-existant transaction/dispute
                    warn!(
                        "[Client {}] resolve does not reference valid outstanding disputed deposit/withdrawal transaction; dropping transaction: {:?}",
                        self.client.id, transaction
                    );
                    return;
                }

                let amount = basic_transaction.unwrap().amount.unwrap();

                self.client.held -= amount;
                self.client.available += amount;
            }
            TransactionType::Chargeback => {
                let basic_transaction =
                    self.client
                        .basic_transactions
                        .get(&id)
                        .and_then(|basic_transaction| {
                            self.client.disputes.remove(&id).then(|| basic_transaction)
                        });

                if basic_transaction.is_none() {
                    // fail silently per PDF's instructions due to non-existant transaction/dispute
                    warn!(
                        "[Client {}] resolve does not reference valid outstanding disputed deposit/withdrawal transaction; dropping transaction: {:?}",
                        self.client.id, transaction
                    );
                    return;
                }

                let amount = basic_transaction.unwrap().amount.unwrap();

                self.client.held -= amount;
                self.client.is_locked = true;
            }
        }
    }
}

pub trait PaymentEngine {
    type ProcessError;
    type SnapshotError;

    fn process(&mut self, transaction: Transaction) -> Result<(), Self::ProcessError>;
    fn finalize(self) -> Vec<Result<ClientSnapshot, Self::SnapshotError>>;
}

#[derive(Debug, Default)]
pub struct StreamPaymentEngine {
    client_workers: HashMap<ClientId, JoinHandle<ClientSnapshot>>,
    senders: HashMap<ClientId, Sender<Transaction>>,
    num_enqueued_transactions: usize,
}

impl PaymentEngine for StreamPaymentEngine {
    type ProcessError = SendError<Transaction>;
    type SnapshotError = anyhow::Error;

    fn process(&mut self, mut transaction: Transaction) -> Result<(), Self::ProcessError> {
        transaction.chrono_order = self.num_enqueued_transactions;
        self.num_enqueued_transactions += 1;

        let client_id = transaction.client_id;
        let sender = self.senders.entry(client_id).or_insert_with(|| {
            // TODO (PERF): Would probably be faster to use Ringbuf SPSC bounded channel, but then
            // we need to handle backpressure appropriately... not going to do that in this exercise
            let (sender, receiver) = crossbeam::channel::unbounded::<Transaction>();

            info!("[Client {client_id}] spawning worker");

            self.client_workers.insert(
                client_id,
                // TODO (PERF): threadpool
                std::thread::spawn(move || client_worker(client_id, receiver)),
            );

            sender
        });

        debug!(
            "[Client {client_id}] Enqueueing transaction: {:?}",
            transaction
        );
        sender.send(transaction)
    }

    fn finalize(self) -> Vec<Result<ClientSnapshot, Self::SnapshotError>> {
        // notify workers to finish up...
        drop(self.senders);

        let mut results = Vec::with_capacity(self.client_workers.len());

        for (client_id, handle) in self.client_workers {
            let result = handle
                .join()
                .map_err(|err| anyhow::anyhow!("{client_id} worker failed: {:?}", err));

            results.push(result);
        }

        results
    }
}

fn client_worker(client_id: ClientId, receiver: Receiver<Transaction>) -> ClientSnapshot {
    let mut engine = ClientPaymentEngine::new(client_id);

    while let Ok(transaction) = receiver.recv() {
        debug!("Processing transaction {:?}", transaction);
        engine.process(transaction);
    }

    engine.generate_snapshot()
}
