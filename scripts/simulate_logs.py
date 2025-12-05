#!/usr/bin/env python3
"""Simple log simulator for testing the ingest Lambda endpoint."""
import random
import time
import uuid
import requests

INGEST_URL = "https://7d3oyl9wbe.execute-api.us-east-1.amazonaws.com/prod/ingest"
TENANTS = ["tenant-1", "tenant-2", "tenant-3"]

LOG_TEMPLATES = [
    "User {phone} accessed the system",
    "Customer {phone} logged in from web",
    "Call from {phone} failed authentication",
    "Support ticket opened by {phone}",
]


def random_phone():
    """Generate random phone number."""
    return f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}"


def send_json_log(tenant_id):
    log_text = random.choice(LOG_TEMPLATES).format(phone=random_phone())
    payload = {
        "tenant_id": tenant_id,
        "log_id": f"log-{uuid.uuid4()}",
        "text": log_text,
    }

    try:
        resp = requests.post(
            INGEST_URL,
            headers={"Content-Type": "application/json"},
            json=payload,
            timeout=5,
        )
        print(f"[JSON] {tenant_id} -> {resp.status_code} | {resp.text if resp.status_code >= 400 else 'OK'}")
        return resp
    except Exception as e:
        print(f"[JSON] {tenant_id} -> ERROR: {e}")
        return None


def send_plaintext_log(tenant_id):
    """Send a plaintext log to the ingest endpoint."""
    log_text = random.choice(LOG_TEMPLATES).format(phone=random_phone())

    try:
        resp = requests.post(
            INGEST_URL,
            headers={
                "Content-Type": "text/plain",
                "X-Tenant-ID": tenant_id,
            },
            data=log_text.encode("utf-8"),
            timeout=5,
        )
        print(f"[PLAIN] {tenant_id} -> {resp.status_code} | {resp.text if resp.status_code >= 400 else 'OK'}")
        return resp
    except Exception as e:
        print(f"[PLAIN] {tenant_id} -> ERROR: {e}")
        return None


def simulate(count=10, delay=0.5):
    """Send mixed JSON and plaintext logs."""
    success = 0
    failed = 0

    for i in range(count):
        tenant = random.choice(TENANTS)

        resp = None
        if random.random() < 0.5:
            resp = send_json_log(tenant)
        else:
            resp = send_plaintext_log(tenant)

        if resp and 200 <= resp.status_code < 300:
            success += 1
        else:
            failed += 1

        if i < count - 1:
            time.sleep(delay)

    print(f"\n Success: {success} | Failed: {failed}")


if __name__ == "__main__":
    import sys

    count = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    print(f"Sending {count} logs to {INGEST_URL}\n")
    simulate(count)
