import os
import csv
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pandas as pd
import psycopg2
from faker import Faker
from kafka import KafkaProducer
import time

# --- КОНФИГУРАЦИЯ ---
# PostgreSQL
PG_HOST = "localhost"
PG_PORT = "5432"
PG_DB = "ecom_db"
PG_USER = "user"
PG_PASSWORD = "password"

# Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_CLICKSTREAM_TOPIC = 'clickstream_events'
KAFKA_ORDERS_TOPIC = 'order_status_updates'

# S3 (MinIO)
S3_ENDPOINT_URL = 'http://localhost:9000'
S3_ACCESS_KEY = 'minioadmin'
S3_SECRET_KEY = 'minioadmin'
S3_BUCKET_NAME = 'company-datalake'
S3_REGION = 'us-east-1' # Неважно для MinIO, но boto3 требует

# Настройки генерации
NUM_CUSTOMERS = 200
NUM_PRODUCTS_A = 100
NUM_ORDERS_A = 500

NUM_CLIENTS = 300
NUM_ITEMS_B = 80
NUM_PURCHASES_B = 400

NUM_KAFKA_CLICKSTREAM_EVENTS = 1000

fake = Faker()

primegoods_customer_ids = []
primegoods_product_ids = []
primegoods_order_ids = []

electroworld_client_uuids = []
electroworld_item_skus = []
electroworld_purchase_guids = []


def get_pg_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD
    )

def setup_postgres_schemas(conn: psycopg2.extensions.connection):
    """Создание схем и таблиц в PostgreSQL"""
    with conn.cursor() as cur:
        print("Создание схем и таблиц для магазинов A и B...")
        # Схема A
        cur.execute("CREATE SCHEMA IF NOT EXISTS primegoods;")

        cur.execute("""
            drop table if exists primegoods.customers;
            drop table if exists primegoods.products;
            drop table if exists primegoods.orders;
            drop table if exists primegoods.order_items;       
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS primegoods.customers (
                customer_id SERIAL PRIMARY KEY, first_name VARCHAR, last_name VARCHAR,
                email VARCHAR, registration_date DATE, address TEXT
            );
            CREATE TABLE IF NOT EXISTS primegoods.products (
                product_id SERIAL PRIMARY KEY, product_name VARCHAR, category VARCHAR,
                price DECIMAL(10, 2), created_at TIMESTAMP
            );
            CREATE TABLE IF NOT EXISTS primegoods.orders (
                order_id SERIAL PRIMARY KEY, customer_id INTEGER, order_date TIMESTAMP,
                status VARCHAR, total_amount DECIMAL(10, 2)
            );
            CREATE TABLE IF NOT EXISTS primegoods.order_items (
                order_item_id SERIAL PRIMARY KEY, order_id INTEGER, product_id INTEGER,
                quantity INTEGER, price_per_unit DECIMAL(10, 2)
            );
        """)
        # Схема B
        cur.execute("CREATE SCHEMA IF NOT EXISTS electroworld;")
        cur.execute("""
            drop table if exists primegoods.clients;
            drop table if exists primegoods.items;
            drop table if exists primegoods.purchases;
            drop table if exists primegoods.purchase_details;       
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS electroworld.clients (
                client_uuid UUID PRIMARY KEY, name VARCHAR, email VARCHAR,
                signup_ts TIMESTAMP WITH TIME ZONE, shipping_info JSONB
            );
            CREATE TABLE IF NOT EXISTS electroworld.items (
                item_sku VARCHAR PRIMARY KEY, item_title VARCHAR, department VARCHAR,
                cost NUMERIC, specs JSONB
            );
            CREATE TABLE IF NOT EXISTS electroworld.purchases (
                purchase_guid UUID PRIMARY KEY, client_ref UUID, purchase_timestamp TIMESTAMP WITH TIME ZONE,
                state INTEGER, total_usd NUMERIC
            );
            CREATE TABLE IF NOT EXISTS electroworld.purchase_details (
                id BIGSERIAL PRIMARY KEY, purchase_guid UUID, item_sku VARCHAR,
                amount INTEGER, price NUMERIC
            );
        """)
        conn.commit()
    print("Схемы и таблицы успешно созданы.")

def generate_primegoods_data(conn: psycopg2.extensions.connection):
    """Генерация данных для магазина A"""

    import psycopg2.extras

    # call it in any place of your program
    # before working with UUID objects in PostgreSQL
    psycopg2.extras.register_uuid()
    with conn.cursor() as cur:
        print("Генерация данных для магазина A (primegoods)...")
        # Customers
        customers = []
        for i in range(1, NUM_CUSTOMERS + 1):
            primegoods_customer_ids.append(i)
            customers.append((
                i, fake.first_name(), fake.last_name(), fake.email(),
                fake.date_between(start_date='-2y', end_date='today'), fake.address()
            ))
        cur.executemany("INSERT INTO primegoods.customers VALUES (%s, %s, %s, %s, %s, %s)", customers)

        # Products
        products = []
        categories = ['Books', 'Home & Kitchen', 'Clothing', 'Electronics', 'Toys']
        for i in range(1, NUM_PRODUCTS_A + 1):
            primegoods_product_ids.append(i)
            products.append((
                i, f"Product Name {i}", random.choice(categories),
                Decimal(random.uniform(5.0, 500.0)), fake.past_datetime(start_date='-2y')
            ))
        cur.executemany("INSERT INTO primegoods.products VALUES (%s, %s, %s, %s, %s)", products)

        # Orders & Order Items
        orders = []
        order_items = []
        for i in range(1, NUM_ORDERS_A + 1):
            primegoods_order_ids.append(i)
            customer_id = random.choice(primegoods_customer_ids)
            order_date = fake.date_time_between(start_date='-1y', end_date='now', tzinfo=timezone.utc)
            total_amount = Decimal(0)

            # Generate order items for this order
            num_items_in_order = random.randint(1, 5)
            for _ in range(num_items_in_order):
                product_id = random.choice(primegoods_product_ids)
                # Fetch product price - in a real scenario this might be complex
                cur.execute("SELECT price FROM primegoods.products WHERE product_id = %s", (product_id,))
                row = cur.fetchone()
                price = 0
                if row:
                    price = row[0]
                else:
                    raise Exception("Некорректно спрсились данные или нет данных из sql запроса. Перепроверьте.")
                quantity = random.randint(1, 3)
                total_amount += price * quantity
                order_items.append((i, product_id, quantity, price))
            
            orders.append((
                i, customer_id, order_date,
                random.choice(['created', 'paid', 'shipped', 'delivered', 'cancelled']),
                total_amount
            ))

        cur.executemany("INSERT INTO primegoods.orders VALUES (%s, %s, %s, %s, %s)", orders)
        cur.executemany("INSERT INTO primegoods.order_items (order_id, product_id, quantity, price_per_unit) VALUES (%s, %s, %s, %s)", order_items)
        conn.commit()
    print(f"Магазин A: {len(customers)} клиентов, {len(products)} товаров, {len(orders)} заказов сгенерировано.")


def generate_electroworld_data(conn: psycopg2.extensions.connection):
    """Генерация данных для магазина B"""
    with conn.cursor() as cur:
        print("Генерация данных для магазина B (electroworld)...")
        # Clients
        clients = []
        for _ in range(NUM_CLIENTS):
            client_uuid = uuid.uuid4()
            electroworld_client_uuids.append(client_uuid)
            clients.append((
                client_uuid, fake.name(), fake.email(),
                fake.date_time_between(start_date='-2y', end_date='now', tzinfo=timezone.utc),
                json.dumps({"city": fake.city(), "street": fake.street_address(), "zip": fake.zipcode()})
            ))
        cur.executemany("INSERT INTO electroworld.clients VALUES (%s, %s, %s, %s, %s)", clients)

        # Items
        items = []
        departments = ['Smartphones', 'Laptops', 'Audio', 'Accessories']
        for i in range(NUM_ITEMS_B):
            sku = f"ELEC-{str(i+1).zfill(4)}"
            electroworld_item_skus.append(sku)
            items.append((
                sku, f"Electronic Item {sku}", random.choice(departments),
                Decimal(random.uniform(50.0, 2000.0)),
                json.dumps({"ram": f"{random.choice([4,8,16,32])}GB", "storage": f"{random.choice([128,256,512,1024])}GB"})
            ))
        cur.executemany("INSERT INTO electroworld.items VALUES (%s, %s, %s, %s, %s)", items)

        purchases = []
        purchase_details = []
        for _ in range(NUM_PURCHASES_B):
            purchase_guid = uuid.uuid4()
            electroworld_purchase_guids.append(purchase_guid)
            client_ref = random.choice(electroworld_client_uuids)
            purchase_ts = fake.date_time_between(start_date='-1y', end_date='now', tzinfo=timezone.utc)
            total_usd = Decimal(0)

            num_items_in_purchase = random.randint(1, 3)
            for _ in range(num_items_in_purchase):
                item_sku = random.choice(electroworld_item_skus)
                cur.execute("SELECT cost FROM electroworld.items WHERE item_sku = %s", (item_sku,))
                row = cur.fetchone()
                price = 0
                if row:
                    price = row[0]
                else:
                    raise Exception("Некорректно спрсились данные или нет данных из sql запроса. Перепроверьте.")
                amount = random.randint(1, 2)
                total_usd += price * amount
                purchase_details.append((purchase_guid, item_sku, amount, price))
            
            purchases.append((
                purchase_guid, client_ref, purchase_ts,
                random.randint(0, 5), total_usd
            ))
        
        cur.executemany("INSERT INTO electroworld.purchases VALUES (%s, %s, %s, %s, %s)", purchases)
        cur.executemany("INSERT INTO electroworld.purchase_details (purchase_guid, item_sku, amount, price) VALUES (%s, %s, %s, %s)", purchase_details)
        conn.commit()
    print(f"Магазин B: {len(clients)} клиентов, {len(items)} товаров, {len(purchases)} покупок сгенерировано.")


def get_s3_client():
    """Подключение к S3 (MinIO)"""
    return boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION
    )

def generate_s3_data(s3_client):
    """Генерация данных для S3"""
    print("Генерация данных для S3...")
    # Product Reviews
    for i in range(300): # 300 отзывов
        is_primegoods = random.random() > 0.5
        shop_id = "primegoods" if is_primegoods else "electroworld"
        product_id = str(random.choice(primegoods_product_ids)) if is_primegoods else random.choice(electroworld_item_skus)
        author_id = str(random.choice(primegoods_customer_ids)) if is_primegoods else str(random.choice(electroworld_client_uuids))
        
        review = {
            "review_id": f"rev-{uuid.uuid4()}",
            "product_id_local": product_id,
            "author_id": author_id,
            "rating": random.randint(1, 5),
            "review_text": fake.paragraph(nb_sentences=3),
            "created_date": fake.date_this_year().isoformat()
        }
        
        file_key = f"raw/product_reviews/shop_id={shop_id}/product_id={product_id}/rev-{i}.json"
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_key, Body=json.dumps(review).encode('utf-8'))

    # Supplier Feeds
    # Shop A - CSV
    supplier_a_data = []
    for product_id in primegoods_product_ids:
        supplier_a_data.append({
            'product_identifier': product_id,
            'supplier_name': random.choice(['SupplierX', 'SupplierY']),
            'stock_count': random.randint(0, 200),
            'cost_price_eur': round(random.uniform(2.0, 400.0), 2)
        })
    df_a = pd.DataFrame(supplier_a_data)
    csv_buffer = df_a.to_csv(index=False)
    file_key_a = f"raw/supplier_feeds/primegoods_suppliers/{datetime.now().strftime('%Y-%m-%d')}.csv"
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_key_a, Body=csv_buffer)
    
    # Shop B - Parquet
    supplier_b_data = []
    for sku in electroworld_item_skus:
        supplier_b_data.append({
            'sku': sku,
            'inventory': random.randint(0, 100),
            'wholesale_price_usd': round(random.uniform(40.0, 1800.0), 2),
            'delivery_time_days': random.randint(1, 10)
        })
    df_b = pd.DataFrame(supplier_b_data)
    parquet_buffer = df_b.to_parquet(index=False)
    file_key_b = f"raw/supplier_feeds/electroworld_vendors/SuperElectronics/data.parquet"
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=file_key_b, Body=parquet_buffer)


def get_kafka_producer():
    """Подключение к Kafka"""
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_kafka_data(producer):
    """Генерация данных для Kafka"""
    print(f"Генерация {NUM_KAFKA_CLICKSTREAM_EVENTS} событий для Kafka...")
    # Clickstream events
    for _ in range(NUM_KAFKA_CLICKSTREAM_EVENTS):
        time.sleep(10)
        is_primegoods = random.random() > 0.5
        shop_id = "primegoods" if is_primegoods else "electroworld"
        user_id = str(random.choice(primegoods_customer_ids)) if is_primegoods else str(random.choice(electroworld_client_uuids))
        product_id = str(random.choice(primegoods_product_ids)) if is_primegoods else random.choice(electroworld_item_skus)
        
        event = {
          "event_id": str(uuid.uuid4()),
          "event_timestamp": datetime.now(timezone.utc).isoformat(),
          "event_type": random.choice(['page_view', 'add_to_cart', 'search']),
          "user_id": user_id,
          "shop_origin": shop_id,
          "payload": {
            "product_id": product_id,
            "page_url": f"/products/{product_id}",
            "user_agent": fake.user_agent()
          }
        }
        producer.send(KAFKA_CLICKSTREAM_TOPIC, event)
    
    # Order status updates
    all_orders = [(o, 'primegoods') for o in primegoods_order_ids] + [(p, 'electroworld') for p in electroworld_purchase_guids]
    for order_id, shop_name in random.sample(all_orders, k=min(len(all_orders), 100)):
        time.sleep(10)
        update = {
          "update_id": f"upd-{uuid.uuid4()}",
          "order_id": str(order_id),
          "shop_id": shop_name,
          "new_status": random.choice(['packed', 'shipped', 'in_transit']),
          "updated_at": datetime.now(timezone.utc).isoformat(),
          "details": {
            "tracking_number": f"TRK{random.randint(100000, 999999)}",
          }
        }
        producer.send(KAFKA_ORDERS_TOPIC, update)

    producer.flush()
    print("Данные в Kafka отправлены.")


if __name__ == '__main__':
    # Шаг 1: PostgreSQL
    pg_conn = get_pg_connection()
    print(type(pg_conn))
    setup_postgres_schemas(pg_conn)
    generate_primegoods_data(pg_conn)
    generate_electroworld_data(pg_conn)
    pg_conn.close()
    
    # Шаг 2: S3 (MinIO)
    s3_client = get_s3_client()
    generate_s3_data(s3_client)

    # Шаг 3: Kafka
    # kafka_producer = get_kafka_producer()
    # generate_kafka_data(kafka_producer)
    
    print("\nГенерация данных завершена!")
    print("Веб-интерфейс MinIO (S3) доступен по адресу: http://localhost:9001")