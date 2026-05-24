# -*- coding: utf-8 -*-
"""
Created on Sat Apr 26 19:27:20 2025

@author: atharv
"""

import random
import time
from datetime import datetime
from kafka import KafkaProducer
import json

# Kafka configuration
KAFKA_TOPIC = "stock_prices"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"  # Default, since you're running locally

# List of stocks
STOCKS = {
    "AAPL": 150.00,
    "TSLA": 700.00,
    "MSFT": 300.00,
    "AMZN": 3200.00,
    "GOOGL": 2800.00
}

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def update_price(price):
    # Fluctuate price randomly by up to ±1%
    change_percent = random.uniform(-0.01, 0.01)
    return round(price * (1 + change_percent), 2)

def produce_stock_prices():
    while True:
        for symbol in STOCKS.keys():
            STOCKS[symbol] = update_price(STOCKS[symbol])

            stock_data = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "price": STOCKS[symbol]
            }

            # Send to Kafka
            producer.send(KAFKA_TOPIC, stock_data)
            print(f"Produced: {stock_data}")

        time.sleep(2)  # Sleep for 2 seconds before next push (you can adjust this)

if __name__ == "__main__":
    try:
        produce_stock_prices()
    except KeyboardInterrupt:
        print("Stopped manually. Closing producer...")
    finally:
        producer.close()
