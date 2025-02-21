// These aren't even remotely close to being comprehensive, but it's
// good enough for now.

#[cfg(test)]
mod integration_tests {
    use payment_engine::{
        ClientSnapshot, Transaction,
        engine::{Engine, PaymentEngine},
        parse::Amount,
    };

    #[test]
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

        for transaction in transactions {
            let result = engine.process(transaction);
            assert!(result.is_ok());
        }

        let mut snapshots = engine
            .finalize()
            .into_iter()
            .filter_map(|result| result.ok())
            .collect::<Vec<_>>();

        snapshots.sort_by_key(|snapshot| snapshot.client);

        let mut snapshots = snapshots.into_iter();

        let client_1_snapshot = snapshots.next().unwrap();
        let client_2_snapshot = snapshots.next().unwrap();

        assert_eq!(1, client_1_snapshot.client);
        assert_eq!(2, client_2_snapshot.client);

        assert_eq!(
            ClientSnapshot {
                client: 1,
                available: Amount::new(7.5).unwrap(),
                held: Amount::new(0.0).unwrap(),
                total: Amount::new(7.5).unwrap(),
                locked: true,
            },
            client_1_snapshot
        );

        assert_eq!(
            ClientSnapshot {
                client: 2,
                available: Amount::new(9.0).unwrap(),
                held: Amount::new(0.0).unwrap(),
                total: Amount::new(9.0).unwrap(),
                locked: false,
            },
            client_2_snapshot
        );
    }
}
