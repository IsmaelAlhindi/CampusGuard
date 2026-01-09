import json
import os
import time
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client("s3")
sns = boto3.client("sns")

# ✅ These must match your environment variables EXACTLY
HISTORY_BUCKET = os.environ["HISTORY_BUCKET"]

TOPIC_EMERGENCY = os.environ["TOPIC_EMERGENCY"]
TOPIC_INFORMATION = os.environ["TOPIC_INFORMATION"]
TOPIC_MAINTENANCE = os.environ["TOPIC_MAINTENANCE"]
TOPIC_TEST = os.environ["TOPIC_TEST"]

TYPE_TO_TOPIC = {
    "Emergency": TOPIC_EMERGENCY,
    "Information": TOPIC_INFORMATION,
    "Maintenance": TOPIC_MAINTENANCE,
    "Test": TOPIC_TEST
}

def write_history(folder, data):
    """Write audit/error logs into the history bucket."""
    key = f"{folder}/{int(time.time())}.json"
    s3.put_object(
        Bucket=HISTORY_BUCKET,
        Key=key,
        Body=json.dumps(data, indent=2).encode("utf-8")
    )

def publish_alert(alert_type, title, message, source):
    """Publish an alert to SNS + write audit log."""
    topic_arn = TYPE_TO_TOPIC.get(alert_type)
    if not topic_arn:
        raise ValueError(f"Unknown alert type: {alert_type}")

    full_message = f"[{alert_type}] {title}\n\n{message}"

    sns.publish(
        TopicArn=topic_arn,
        Subject=f"CampusGuard: {alert_type} Alert",
        Message=full_message
    )

    write_history("Audit", {
        "timestamp": int(time.time()),
        "source": source,
        "type": alert_type,
        "title": title,
        "status": "SENT"
    })

def lambda_handler(event, context):
    print("EVENT:", json.dumps(event))

    # =========================
    # A) Trigger 1: S3 upload
    # =========================
    if "Records" in event and event["Records"][0].get("eventSource") == "aws:s3":
        rec = event["Records"][0]
        bucket = rec["s3"]["bucket"]["name"]
        key = unquote_plus(rec["s3"]["object"]["key"])  # handles spaces/special chars

        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read().decode("utf-8")
            alert = json.loads(body)

            # Required JSON fields
            alert_type = alert["type"]      # Emergency / Information / Maintenance / Test
            title = alert["title"]
            message = alert["message"]

            publish_alert(alert_type, title, message, source="S3Upload")

            # Move file to processed/
            processed_key = key.replace("Incoming/", "Processed/", 1)
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=processed_key
            )
            s3.delete_object(Bucket=bucket, Key=key)

            return {"ok": True, "status": "sent"}

        except Exception as e:
            print("ERROR:", str(e))

            # Move file to failed/
            failed_key = key.replace("Incoming/", "Failed/", 1)
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=failed_key
            )
            s3.delete_object(Bucket=bucket, Key=key)

            write_history("Errors", {
                "timestamp": int(time.time()),
                "source": "S3Upload",
                "bucket": bucket,
                "key": key,
                "status": "FAILED",
                "error": str(e)
            })
            raise

    # =========================
    # B) Trigger 2: EventBridge schedule
    # =========================
    if event.get("source") == "eventbridge.schedule":
        alert_type = event.get("type", "Test")
        title = event.get("title", "Scheduled Test")
        message = event.get("message", "This is a scheduled alert from CampusGuard.")
        publish_alert(alert_type, title, message, source="EventBridgeSchedule")
        return {"ok": True, "status": "scheduled_sent"}

    return {"ok": True, "status": "ignored"}
