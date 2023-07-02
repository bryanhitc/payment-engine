use crossbeam::channel::{Receiver, SendError, Sender};
use log::info;
use std::{collections::HashMap, thread::JoinHandle};

use super::*;

pub type Engine = StreamPaymentEngine;

// Streams transactions to client-partitioned worker threads for async processing.
// This allows the main thread to continue adding transactions while worker threads
// do the actual processing. This is almost certaintly slower than the SerialPaymentEngine
// for this example problem, but I want to show that I understand how this can be done
// if transaction processing was more expensive (e.g., database calls, more compute-heavy
// calculations, etc.)
#[derive(Debug, Default)]
pub struct StreamPaymentEngine {
    client_workers: HashMap<ClientId, JoinHandle<Result<ClientSnapshot, TransactionProcessError>>>,
    senders: HashMap<ClientId, Sender<Transaction>>,
    num_enqueued_transactions: usize,
}

impl StreamPaymentEngine {
    fn client_worker_thread(
        client_id: ClientId,
        receiver: Receiver<Transaction>,
    ) -> Result<ClientSnapshot, TransactionProcessError> {
        let client_manager = SingleClientManager::new(client_id);
        let mut processor = TransactionProcessor::new(client_manager);
        while let Ok(transaction) = receiver.recv() {
            if let Err(err) = processor.process(transaction) {
                // Silently fail + log if business logic error per PDF instructions
                error!("{}", err);
                if let TransactionProcessError::Unknown = err {
                    return Err(err);
                }
            };
        }

        Ok(processor.get_client_manager().generate_snapshot())
    }
}

impl PaymentEngine for StreamPaymentEngine {
    type ProcessError = SendError<Transaction>;
    type SnapshotError = TransactionProcessError;

    fn process(&mut self, transaction: Transaction) -> Result<(), Self::ProcessError> {
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
        for handle in self.client_workers.into_values() {
            let result = handle
                .join()
                .unwrap_or(Err(TransactionProcessError::Unknown));
            results.push(result);
        }
        results
    }
}

#[derive(Debug)]
struct SingleClientManager {
    client: Client,
}

impl SingleClientManager {
    pub fn new(client_id: ClientId) -> Self {
        Self {
            client: Client::new(client_id),
        }
    }

    pub fn generate_snapshot(&self) -> ClientSnapshot {
        ClientSnapshot::from(&self.client)
    }
}

impl ClientManager for SingleClientManager {
    fn get_or_insert_client_mut(&mut self, _client_id: ClientId) -> &mut Client {
        &mut self.client
    }
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
}
