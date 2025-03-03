// These aren't even remotely close to being comprehensive, but it's
// good enough for now.

#[cfg(test)]
mod integration_tests {
    use googletest::prelude::*;
    use payment_engine::{
        ClientSnapshot, Transaction,
        engine::{Engine, PaymentEngine},
        parse::Amount,
    };

    #[gtest]
    fn integration() {
        let mut engine = Engine::default();
        let transactions = [
            Transaction {
                id: 1,
                client_id: 1,
                action: payment_engine::TransactionType::Deposit,
                amount: Amount::new(3.0).ok(),
            },
            Transaction {
                id: 2,
                client_id: 1,
                action: payment_engine::TransactionType::Withdrawal,
                amount: Amount::new(1.5).ok(),
            },
            Transaction {
                id: 3,
                client_id: 1,
                action: payment_engine::TransactionType::Deposit,
                amount: Amount::new(4.5).ok(),
            },
            Transaction {
                id: 4,
                client_id: 2,
                action: payment_engine::TransactionType::Deposit,
                amount: Amount::new(9.0).ok(),
            },
            // this will not go through because wrong client id
            Transaction {
                id: 4,
                client_id: 1,
                action: payment_engine::TransactionType::Dispute,
                amount: None,
            },
            // this will not go through because wrong client id
            Transaction {
                id: 4,
                client_id: 1,
                action: payment_engine::TransactionType::Chargeback,
                amount: None,
            },
            Transaction {
                id: 2,
                client_id: 1,
                action: payment_engine::TransactionType::Chargeback,
                amount: None,
            },
            Transaction {
                id: 2,
                client_id: 1,
                action: payment_engine::TransactionType::Dispute,
                amount: None,
            },
            Transaction {
                id: 2,
                client_id: 1,
                action: payment_engine::TransactionType::Chargeback,
                amount: None,
            },
            Transaction {
                id: 5,
                client_id: 1,
                action: payment_engine::TransactionType::Deposit,
                amount: Amount::new(100.0).ok(),
            },
            Transaction {
                id: 6,
                client_id: 1,
                action: payment_engine::TransactionType::Withdrawal,
                amount: Amount::new(30.0).ok(),
            },
        ];

        assert_that!(transactions.map(|t| engine.process(t)), each(ok(())));
        expect_that!(
            engine.finalize(),
            unordered_elements_are!(
                ok(eq(&ClientSnapshot {
                    client: 1,
                    available: Amount::new(7.5).unwrap(),
                    held: Amount::new(0.0).unwrap(),
                    total: Amount::new(7.5).unwrap(),
                    locked: true,
                })),
                ok(eq(&ClientSnapshot {
                    client: 2,
                    available: Amount::new(9.0).unwrap(),
                    held: Amount::new(0.0).unwrap(),
                    total: Amount::new(9.0).unwrap(),
                    locked: false,
                }))
            )
        );
    }
}
