import json
import time
import random
import boto3
from datetime import datetime

# Boto3 client configuration pointing to LocalStack Kinesis
kinesis_client = boto3.client(
    'kinesis',
    endpoint_url='http://localhost:4566',
    region_name='us-east-1',
    aws_access_key_id='mock',
    aws_secret_access_key='mock'
)

STREAM_NAME = 'credit_applications_stream'

def init_stream():
    """Creates the Kinesis stream in LocalStack if it doesn't exist."""
    try:
        kinesis_client.create_stream(StreamName=STREAM_NAME, ShardCount=1)
        print(f"✅ Stream '{STREAM_NAME}' created successfully.")
        time.sleep(2)  # Wait for stream activation
    except kinesis_client.exceptions.ResourceInUseException:
        print(f"ℹ️ Stream '{STREAM_NAME}' already exists.")

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

def run_generator():
    init_stream()
    print("🚀 Real-time credit streaming engine started...")
    
    last_payload = None
    
    while True:
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
            print(f"📥 Dispatched Event: {payload['application_id']} | Amount: ${payload['requested_amount']}")
        except Exception as e:
            print(f"❌ Kinesis stream error: {e}")
            
        time.sleep(random.uniform(0.5, 2.0))

if __name__ == '__main__':
    run_generator()