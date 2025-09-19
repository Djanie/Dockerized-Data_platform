import pandas as pd
from faker import Faker
import random
import os

fake = Faker()

def generate_orders_csv(output_path='data/incoming/orders.csv', num_records=1000):
    records = []
    cuisines = ['Italian', 'Chinese', 'Mexican', 'Indian', 'American']

    for i in range(num_records):
        order_time = fake.date_time_this_year()
        delivery_time = fake.date_time_between(start_date=order_time)
        delivery_duration = (delivery_time - order_time).total_seconds() / 60
        record = {
            'order_id': fake.uuid4(),
            'restaurant_id': random.randint(1, 100),
            'customer_id': random.randint(1, 500),
            'cuisine': random.choice(cuisines),
            'order_time': order_time,
            'delivery_time': delivery_time,
            'delivery_duration_minutes': delivery_duration,
            'distance_km': round(random.uniform(0.1, 20.0), 2),
            'rating': random.choice([1, 2, 3, 4, 5, None]),
            'amount': round(random.uniform(10, 200), 2)
        }
        # introduce 2% nulls randomly
        if random.random() < 0.02:
            record['rating'] = None
        records.append(record)

    df = pd.DataFrame(records)

    # Introduce duplicates around 5%
    duplicates = df.sample(frac=0.05)
    df = pd.concat([df, duplicates], ignore_index=True)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Generated {len(df)} records to {output_path}")

if __name__ == '__main__':
    generate_orders_csv()
