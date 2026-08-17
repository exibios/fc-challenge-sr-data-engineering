"""
Lightweight Kinesis status/peek utility for LocalStack.
Uses boto3 pointed at LocalStack (no aws cli required).

Outputs stream description, shard ids, and a small sample count per shard
(from TRIM_HORIZON) to help approximate backlog for local testing.
"""
import os
import boto3

ENDPOINT = os.getenv("KINESIS_ENDPOINT_URL", "http://localhost:4566")
STREAM = os.getenv("KINESIS_STREAM_NAME", "credit_applications_stream")
SAMPLE_LIMIT = int(os.getenv("KINESIS_SAMPLE_LIMIT", "100"))


def client():
    return boto3.client(
        "kinesis",
        endpoint_url=ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id=os.getenv("KINESIS_ACCESS_KEY_ID", "mock"),
        aws_secret_access_key=os.getenv("KINESIS_SECRET_ACCESS_KEY", "mock"),
    )


def main():
    c = client()
    try:
        desc = c.describe_stream_summary(StreamName=STREAM)
    except Exception as e:
        print(f"Error describing stream '{STREAM}': {e}")
        return

    print("Stream summary:")
    print(desc)

    try:
        shards = c.list_shards(StreamName=STREAM).get("Shards", [])
    except Exception as e:
        print(f"Error listing shards: {e}")
        return

    if not shards:
        print("No shards found")
        return

    print("Shards:")
    for s in shards:
        shard_id = s["ShardId"]
        print(f"- {shard_id}")

        try:
            it = c.get_shard_iterator(StreamName=STREAM, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
            recs = c.get_records(ShardIterator=it, Limit=SAMPLE_LIMIT).get("Records", [])
            print(f"  approx sample records (up to {SAMPLE_LIMIT}): {len(recs)}")
        except Exception as e:
            print(f"  could not sample records for shard {shard_id}: {e}")


if __name__ == "__main__":
    main()
