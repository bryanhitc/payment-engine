use std::thread::JoinHandle;

use crossbeam::channel::{Receiver, SendError, Sender};
use log::{debug, info, warn};

use crate::*;

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

// Used by StreamPaymentEngine
#[derive(Debug)]
pub struct SingleClientManager {
    client: Client,
}

impl SingleClientManager {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client: Client::new(client_id),
        }
    }

    fn generate_snapshot(&self) -> ClientSnapshot {
        ClientSnapshot::from(&self.client)
    }
}

impl ClientManager for SingleClientManager {
    fn get_or_insert_client_mut(&mut self, _client_id: ClientId) -> &mut Client {
        &mut self.client
    }
}

// Used by SerialPaymentEngine
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

// Contains the core business logic for processing transactions
#[derive(Debug)]
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
    fn new(client_manager: C) -> Self {
        TransactionProcessor { client_manager }
    }

    fn get_client_manager(&self) -> &C {
        &self.client_manager
    }

    fn process(&mut self, transaction: Transaction) {
        let client = self
            .client_manager
            .get_or_insert_client_mut(transaction.client_id);

        if client.is_locked {
            // In a real system, we probably don't want to drop a transaction
            // if the account is locked, but rather keep it in a separate queue.
            // I'm just going to drop it for this coding exercise, though :)
            warn!(
                "[Client {}] account is locked; dropping transaction {:?}",
                client.id, transaction
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

                client.available += amount;
                client
                    .basic_transactions
                    .insert(transaction.id, transaction);
            }
            TransactionType::Withdrawal => {
                let amount = transaction.amount.unwrap();

                if client.available < amount {
                    // failing silently per PDF's instructions...
                    warn!(
                        "[Client {}] insufficient funds; dropping transaction: {:?}",
                        client.id, transaction
                    );
                    return;
                }

                client.available -= amount;
                client
                    .basic_transactions
                    .insert(transaction.id, transaction);
            }
            TransactionType::Dispute => {
                let basic_transaction = client.basic_transactions.get(&id);

                if basic_transaction.is_none() {
                    // failing silently per PDF's instructions
                    warn!(
                        "[Client {}] dispute does not reference valid deposit/withdrawal transaction; dropping transaction: {:?}",
                        client.id, transaction
                    );
                    return;
                }

                let basic_transaction = basic_transaction.unwrap();
                let is_duplicate_dispute = !client.disputes.insert(transaction.id);

                if is_duplicate_dispute {
                    warn!(
                        "[Client {}] received duplicate dispute; dropping transaction: {:?}",
                        client.id, transaction
                    );
                    return;
                }

                let amount = basic_transaction.amount.unwrap();

                client.available -= amount;
                client.held += amount;
            }
            TransactionType::Resolve => {
                let basic_transaction =
                    client
                        .basic_transactions
                        .get(&id)
                        .and_then(|basic_transaction| {
                            client.disputes.remove(&id).then(|| basic_transaction)
                        });

                if basic_transaction.is_none() {
                    // fail silently per PDF's instructions due to non-existant transaction/dispute
                    warn!(
                        "[Client {}] resolve does not reference valid outstanding disputed deposit/withdrawal transaction; dropping transaction: {:?}",
                        client.id, transaction
                    );
                    return;
                }

                let amount = basic_transaction.unwrap().amount.unwrap();

                client.held -= amount;
                client.available += amount;
            }
            TransactionType::Chargeback => {
                let basic_transaction =
                    client
                        .basic_transactions
                        .get(&id)
                        .and_then(|basic_transaction| {
                            client.disputes.remove(&id).then(|| basic_transaction)
                        });

                if basic_transaction.is_none() {
                    // fail silently per PDF's instructions due to non-existant transaction/dispute
                    warn!(
                        "[Client {}] resolve does not reference valid outstanding disputed deposit/withdrawal transaction; dropping transaction: {:?}",
                        client.id, transaction
                    );
                    return;
                }

                let amount = basic_transaction.unwrap().amount.unwrap();

                client.held -= amount;
                client.is_locked = true;
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

// Processes transactions immediately/syncronously
#[derive(Debug)]
pub struct SerialPaymentEngine {
    processor: TransactionProcessor<MultiClientManager>,
}

impl PaymentEngine for SerialPaymentEngine {
    type ProcessError = anyhow::Error;
    type SnapshotError = anyhow::Error;

    fn process(&mut self, transaction: Transaction) -> Result<(), Self::ProcessError> {
        self.processor.process(transaction);
        Ok(())
    }

    fn finalize(self) -> Vec<Result<ClientSnapshot, Self::SnapshotError>> {
        let clients = self.processor.client_manager.clients;
        let mut results = Vec::with_capacity(clients.len());

        for (_, client) in clients {
            results.push(Ok(ClientSnapshot::from(&client)));
        }

        results
    }
}

impl Default for SerialPaymentEngine {
    fn default() -> Self {
        Self {
            processor: TransactionProcessor {
                client_manager: MultiClientManager::default(),
            },
        }
    }
}

// Streams transactions to client-partitioned worker threads for async processing.
// This allows the main thread to continue adding transactions while worker threads
// do the actual processing. This is almost certaintly slower than the SerialPaymentEngine
// for this example problem, but I want to show that I understand how this can be done
// if transaction processing was more expensive (e.g., database calls, more compute-heavy
// calculations, etc.)
#[derive(Debug, Default)]
pub struct StreamPaymentEngine {
    client_workers: HashMap<ClientId, JoinHandle<ClientSnapshot>>,
    senders: HashMap<ClientId, Sender<Transaction>>,
    num_enqueued_transactions: usize,
}

impl StreamPaymentEngine {
    fn client_worker_thread(
        client_id: ClientId,
        receiver: Receiver<Transaction>,
    ) -> ClientSnapshot {
        let client_manager = SingleClientManager::new(client_id);
        let mut processor = TransactionProcessor::new(client_manager);

        while let Ok(transaction) = receiver.recv() {
            debug!("Processing transaction {:?}", transaction);
            processor.process(transaction);
        }

        processor.get_client_manager().generate_snapshot()
    }
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
                // TODO (PERF + CORRECTNESS): threadpool, otherwise, we have N threads
                // where N = # unique clients. Obviously, this won't scale.
                std::thread::spawn(move || Self::client_worker_thread(client_id, receiver)),
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
