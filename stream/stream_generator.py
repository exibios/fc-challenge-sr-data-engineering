import json
import time
import random
import os
import boto3
from datetime import datetime

# Configuration from environment (works both on-host and inside containers)
KINESIS_ENDPOINT_URL = os.getenv('KINESIS_ENDPOINT_URL', 'http://localstack:4566')
AWS_ACCESS_KEY_ID = os.getenv('KINESIS_ACCESS_KEY_ID', os.getenv('AWS_ACCESS_KEY_ID', 'mock'))
AWS_SECRET_ACCESS_KEY = os.getenv('KINESIS_SECRET_ACCESS_KEY', os.getenv('AWS_SECRET_ACCESS_KEY', 'mock'))

# Generator control: run for a maximum number of seconds or events (0 = unlimited)
GENERATOR_SECONDS = int(os.getenv('GENERATOR_SECONDS', '0'))
GENERATOR_EVENTS = int(os.getenv('GENERATOR_EVENTS', '0'))

STREAM_NAME = os.getenv('KINESIS_STREAM_NAME', 'credit_applications_stream')

# Boto3 client configuration pointing to LocalStack Kinesis
kinesis_client = boto3.client(
    'kinesis',
    endpoint_url=KINESIS_ENDPOINT_URL,
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

def init_stream():
    """Creates the Kinesis stream in LocalStack if it doesn't exist."""
    try:
        kinesis_client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
        print(f"✅ Stream '{STREAM_NAME}' created successfully.")
        time.sleep(2)  # Wait for stream activation
    except Exception as e:
        # ResourceInUseException (already exists) may be raised; treat as informational
        if 'ResourceInUseException' in str(e) or 'ResourceInUse' in str(e):
            print(f"ℹ️ Stream '{STREAM_NAME}' already exists.")
        else:
            print(f"ℹ️ Stream init note: {e}")

def generate_credit_application():
    """Generates a mock credit application event with intentional occasional anomalies."""
    app_id = f"APP-{random.randint(100000, 999999)}"
    
    anomaly_dice = random.random()
    declared_income = random.randint(15000, 85000)
    requested_amount = random.randint(5000, 50000)
    
    # --- DATA TRAPS ---
    if anomaly_dice < 0.05:
        # Trap 1: Missing critical data field (Should trigger Data Quality rejection)
        declared_income = None
    elif anomaly_dice < 0.10:
        # Trap 2: Corrupt negative value calculation
        requested_amount = -5000
        
    payload = {
        "application_id": app_id,
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "requested_amount": requested_amount,
        "declared_income": declared_income,
        "customer_age": random.randint(18, 65),
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Trap 3: Direct Duplicate Event Injection (Tests Idempotence)
    is_duplicate = random.random() < 0.05
    
    return payload, is_duplicate

def run_generator(max_seconds: int = GENERATOR_SECONDS, max_events: int = GENERATOR_EVENTS):
    init_stream()
    print("🚀 Real-time credit streaming engine started...")

    last_payload = None
    sent_events = 0
    start_time = time.time()

    while True:
        # Termination checks
        if max_events and sent_events >= max_events:
            print(f"🚦 Reached target of {max_events} events. Exiting.")
            break
        if max_seconds and (time.time() - start_time) >= max_seconds:
            print(f"⏱ Reached time limit of {max_seconds} seconds. Exiting.")
            break

        if last_payload and random.random() < 0.5:
            payload = last_payload
            print(f"⚠️ Injecting duplicate record on purpose: {payload['application_id']}")
            last_payload = None
        else:
            payload, is_duplicate = generate_credit_application()
            if is_duplicate:
                last_payload = payload

        try:
            kinesis_client.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=payload['application_id']
            )
            sent_events += 1
            print(f"📥 Dispatched Event: {payload['application_id']} | Amount: ${payload['requested_amount']} (total_sent={sent_events})")
        except Exception as e:
            print(f"❌ Kinesis stream error: {e}")

        time.sleep(random.uniform(0.5, 2.0))

if __name__ == '__main__':
    run_generator()