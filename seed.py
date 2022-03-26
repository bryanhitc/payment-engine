from random import randint, random

num_clients = 1024
num_transactions = 1_000_000
max_precision = 4

print("type, client, tx, amount")

for i, transaction in enumerate(range(num_transactions)):
    client_id = randint(0, num_clients)
    is_deposit = 'deposit' if random() > 0.5 else 'withdrawal'

    amount_precision = randint(0, max_precision)
    amount = round(randint(0, 1000) * random(), amount_precision)

    print(f"{is_deposit}, {client_id}, {i}, {amount}")
