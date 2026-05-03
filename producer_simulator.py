import boto3
import json
import random
import time

# Initialize Kinesis client
client = boto3.client('kinesis', region_name='us-east-1')  # Add your AWS region

users = ["user1", "user2", "user3", "user4"]
actions = ["view", "click", "add_to_cart", "purchase"]

print("Starting producer simulator...")

while True:
    try:
        user_id = random.choice(users)
        action = random.choice(actions)
        event = {
            "user_id": user_id,
            "action": action,
            "timestamp": time.time()
        }

        response = client.put_record(
            StreamName="my-pipeline-stream",
            Data=json.dumps(event).encode("utf-8"),
            PartitionKey=user_id
        )

        print(f"Sent: {event} | ShardID: {response['ShardId']}")
        time.sleep(1)

    except Exception as e:
        print(f"Error sending record: {e}")
        time.sleep(1)  # Wait before retrying
