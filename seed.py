# extremely naive seed data generator
# the way the transaction are spread out => bad for our client-partitioned stream workers

transaction_id = 0

print("type, client, tx, amount")

for client_id in range(1, 10000):
    print(f"deposit, {client_id}, {transaction_id + 1}, 1.0")
    print(f"deposit, {client_id}, {transaction_id + 2}, 5.0")
    print(f"withdrawal, {client_id}, {transaction_id + 3}, 2.0")

    for _ in range(4):
        # premature resolve
        print(f"resolve, {client_id}, {transaction_id + 1}")

    for _ in range(3):
        # duplicate disputes
        print(f"dispute, {client_id}, {transaction_id + 1}")

    for _ in range(4):
        # duplicate resolves
        print(f"resolve, {client_id}, {transaction_id + 1}")

    transaction_id += 3
