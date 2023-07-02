use super::*;

pub type Engine = SerialPaymentEngine;

// Processes transactions immediately/syncronously
#[derive(Debug, Default)]
pub struct SerialPaymentEngine {
    processor: TransactionProcessor<MultiClientManager>,
}

impl PaymentEngine for SerialPaymentEngine {
    type ProcessError = TransactionProcessError;
    type SnapshotError = anyhow::Error;

    fn process(&mut self, transaction: Transaction) -> Result<(), Self::ProcessError> {
        if let Err(err) = self.processor.process(transaction) {
            // Silently fail + log if business logic error per PDF instructions
            error!("{}", err);
            if let TransactionProcessError::Unknown = err {
                return Err(err);
            }
        }

        Ok(())
    }

    fn finalize(self) -> Vec<Result<ClientSnapshot, Self::SnapshotError>> {
        let clients = self.processor.client_manager.clients;
        let mut results = Vec::with_capacity(clients.len());
        for client in clients.values() {
            results.push(Ok(ClientSnapshot::from(client)));
        }
        results
    }
}
