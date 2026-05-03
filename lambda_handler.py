import json
import base64
import boto3
from datetime import datetime, timezone

firehose = boto3.client('firehose')
DELIVERY_STREAM_NAME = "clickstream-firehose"

def lambda_handler(event, context):
    records_to_send = []
    failed_records = 0

    for record in event['Records']:
        try:
            payload = base64.b64decode(record['kinesis']['data'])
            data = json.loads(payload)

            data["processed_at"] = datetime.now(timezone.utc).isoformat()

            if data.get("action") == "purchase":
                print(f"Purchase detected: {data['user_id']}")

            records_to_send.append({
                "Data": (json.dumps(data) + "\n").encode("utf-8")
            })
        except Exception as e:
            print(f"Error processing record: {e}")
            failed_records += 1

    if records_to_send:
        response = firehose.put_record_batch(
            DeliveryStreamName=DELIVERY_STREAM_NAME,
            Records=records_to_send
        )

        if response['FailedPutCount'] > 0:
            print(f"Warning: {response['FailedPutCount']} records failed to deliver to Firehose")
        else:
            print(f"Successfully sent {len(records_to_send)} records to Firehose")

    return {
        "statusCode": 200,
        "records_sent": len(records_to_send),
        "records_failed": failed_records
    }
