# -*- coding: utf-8 -*-
"""
Created on Sat Apr 26 19:57:59 2025

@author: atharv
"""

from kafka import KafkaConsumer
import json
import psycopg2

# Kafka config
KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

# PostgreSQL config
POSTGRES_HOST = "localhost"
POSTGRES_PORT = 5432
POSTGRES_DB = "stock_db"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "atharv2001"  # Change if you have a custom password

# Initialize Kafka consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)
cursor = conn.cursor()
# cursor.execute('SELECT datname FROM pg_database;')
# print(cursor.fetchall())

# Create table if not exists
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS stock_prices (
#     id SERIAL PRIMARY KEY,
#     timestamp TIMESTAMP,
#     symbol VARCHAR(10),
#     price NUMERIC
# )
# """)
# conn.commit()

print("Consumer is running and waiting for messages...")

# Consume messages
try:
    for message in consumer:
        stock_data = message.value
        print(f"Consumed: {stock_data}")

        cursor.execute("""
            INSERT INTO stock_prices (timestamp, symbol, price)
            VALUES (%s, %s, %s)
        """, (stock_data['timestamp'], stock_data['symbol'], stock_data['price']))
        conn.commit()
        print('Values inserted in DB')

except KeyboardInterrupt:
    print("Stopped manually. Closing consumer...")
finally:
    consumer.close()
    cursor.close()
    conn.close()
