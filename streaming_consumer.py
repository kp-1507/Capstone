import json
import random
import time
from datetime import datetime
import os

from azure.eventhub import EventHubProducerClient, EventData
from dotenv import load_dotenv

# ---------------------------
# LOAD ENV VARIABLES
# ---------------------------
load_dotenv()

conn_str = os.getenv("EVENTHUB_CONNECTION_STRING")
hub_name = os.getenv("EVENTHUB_NAME")
batch_size = int(os.getenv("BATCH_SIZE", 5))
sleep_interval = int(os.getenv("SLEEP_INTERVAL", 1))

if not conn_str or not hub_name:
    raise ValueError("❌ Missing EventHub credentials in .env")

# ---------------------------
# SAMPLE DATA
# ---------------------------
products = [f'P{i:04d}' for i in range(1, 201)]
customers = [f'C{i:04d}' for i in range(1, 501)]
cities = ['Mumbai', 'Delhi', 'Bengaluru', 'Chennai', 'Hyderabad']

# ---------------------------
# ORDER GENERATOR
# ---------------------------
def generate_order():
    qty = random.randint(1, 10)
    price = round(random.uniform(50, 50000), 2)

    return {
        "order_id": f"SO{random.randint(100000,999999)}",
        "customer_id": random.choice(customers),
        "product_id": random.choice(products),
        "quantity": qty,
        "unit_price": price,
        "total_amount": round(qty * price, 2),
        "city": random.choice(cities),
        "timestamp": datetime.utcnow().isoformat(),
        "currency": "INR"
    }

# ---------------------------
# PRODUCER INIT
# ---------------------------
producer = EventHubProducerClient.from_connection_string(
    conn_str=conn_str,
    eventhub_name=hub_name
)

print("🚀 Streaming orders to Event Hub... Press Ctrl+C to stop.")

# ---------------------------
# STREAM LOOP
# ---------------------------
try:
    while True:
        batch = producer.create_batch()

        for _ in range(batch_size):
            event = generate_order()
            batch.add(EventData(json.dumps(event)))

        producer.send_batch(batch)

        print(f"✅ Sent {batch_size} orders at {datetime.utcnow().strftime('%H:%M:%S')}")

        time.sleep(sleep_interval)

except KeyboardInterrupt:
    print("🛑 Streaming stopped by user.")

except Exception as e:
    print(f"❌ Error: {e}")

finally:
    producer.close()
    print("🔒 Producer connection closed.")