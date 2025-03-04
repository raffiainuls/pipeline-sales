import json 
import random 
import time
from datetime import datetime
from faker import Faker
from confluent_kafka import Producer
import psycopg2
import os 


fake = Faker()



KAFKA_BROKER = os.getenv("KAFKA_BORKER")
TOPIC_NAME = os.getenv("TOPIC_NAME", "tbl_sales_schema")

Producer_conf = {'bootstrap.servers':KAFKA_BROKER}
producer = Producer(Producer_conf)

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOSTNAME"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DATABASE"),
    "user": os.getenv("POSTGRES_USERNAME"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

def get_list(column, tabel):
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute(f"SELECT {column} FROM {tabel} ORDER BY {column}")
        data = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        return data
    except Exception as e:
        print(f"❌ Gagal mengambil data dari PostgreSQL: {e}")
        return []

def get_max_id():
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(MAX(id), 0) FROM tbl_sales")
        max_id = cur.fetchone()[0]
        cur.close()
        conn.close()
        return max_id
    except Exception as e:
        print(f"❌ Gagal mengambil max ID dari PostgreSQL: {e}")
        return 0

product_list = get_list('id', 'tbl_product')
customer_list = get_list('id', 'tbl_customers')
branch_list = get_list('id', 'tbl_branch')
payment_method_list = get_list('id', 'tbl_payment_method')
order_status_list = get_list('id', 'tbl_order_status')
order_status_list_weights = [0.1, 0.9]
payment_status_list = get_list('id', 'tbl_payment_status')
payment_status_list_weights = [0.05, 0.9, 0.05]
shipping_status_list = get_list('id', 'tbl_shipping_status')
shipping_status_list_weights = [0.05, 0.9, 0.05]

def generate_sales_data():
    num_rows = random.randint(1, 5)
    sales_data = []
    start_id = get_max_id()

    for i in range(num_rows):
        id = start_id + i + 1
        product_id = random.choice(product_list)
        customer_id = random.choice(customer_list)
        branch_id = random.choice(branch_list)
        quantity = random.randint(1, 5)
        payment_method = random.choice(payment_method_list)
        order_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        order_status = random.choices(order_status_list, weights=order_status_list_weights, k=1)[0]
        payment_status = None
        shipping_status = None 
        is_online_transaction = random.choice([True, False])
        delivery_fee = 0
        is_free_delivery_fee = None

        if is_online_transaction:
            delivery_fee = random.randint(12000, 50000)
            is_free_delivery_fee = random.choice([True, False])

        if order_status == 2:
            payment_status = random.choices(payment_status_list, weights=payment_status_list_weights, k=1)[0]
            if payment_status == 2:
                shipping_status = random.choices(shipping_status_list, weights=shipping_status_list_weights, k=1)[0]

        if not is_online_transaction:
            shipping_status = None 

        payload = {
            "id": id,
            "product_id": product_id,
            "customer_id": customer_id,
            "branch_id": branch_id,
            "quantity": quantity,
            "payment_method": payment_method,
            "order_date": order_date,
            "order_status": order_status,
            "payment_status": payment_status,
            "shipping_status": shipping_status,
            "is_online_transaction": is_online_transaction,
            "delivery_fee": delivery_fee,
            "is_free_delivery_fee": is_free_delivery_fee,
            "created_at": order_date, 
            "modified_at": None
        }

        schema = {
            "schema": {
                "fields": [
                    {"type": "int32", "optional": False, "field": "id"},
                    {"type": "int32", "optional": False, "field": "product_id"},
                    {"type": "int32", "optional": False, "field": "customer_id"},
                    {"type": "int32", "optional": False, "field": "branch_id"},
                    {"type": "int32", "optional": False, "field": "quantity"},
                    {"type": "int32", "optional": True, "field": "payment_method"},
                    {"type": "string", "optional": True, "field": "order_date"},
                    {"type": "int32", "optional": True, "field": "order_status"},
                    {"type": "int32", "optional": True, "field": "payment_status"},
                    {"type": "int32", "optional": True, "field": "shipping_status"},
                    {"type": "boolean", "optional": True, "field": "is_online_transaction"},
                    {"type": "int32", "optional": True, "field": "delivery_fee"},
                    {"type": "boolean", "optional": True, "field": "is_free_delivery_fee"},
                    {"type": "string", "optional": True, "field": "created_at"},
                    {"type": "string", "optional": True, "field": "modified_at"},
                ],
                "optional": False,
                "name": "sales_schema",
                "type": "struct"
            },
            "payload": payload
        }
        sales_data.append(schema)
    return sales_data

def delivery_report(err, msg):
    """Callback untuk menangani hasil pengiriman Kafka"""
    if err is not None:
        print(f"❌ Gagal mengirim pesan: {err}")
    else:
        print(f"✅ Data dikirim ke Kafka: {msg.topic()} [{msg.partition()}]")

while True:
    sales_data = generate_sales_data()
    for record in sales_data:
        producer.produce(TOPIC_NAME, json.dumps(record).encode("utf-8"), callback=delivery_report)
    producer.flush()

    print(f"✅ {len(sales_data)} data berhasil dikirim ke Kafka!")
    time.sleep(3600)  # Tunggu 1 jam sebelum mengirim data baru
