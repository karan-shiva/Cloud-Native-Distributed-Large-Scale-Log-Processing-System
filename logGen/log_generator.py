from faker import Faker
import boto3
from http import HTTPStatus
import json
from datetime import datetime
import random
import time

fake = Faker()
services = ["auth-service", "user-serivce", "payment-service", "order-service"]
statuses = [HTTPStatus.OK, 
            HTTPStatus.CREATED,
            HTTPStatus.BAD_REQUEST,
            HTTPStatus.FORBIDDEN,
            HTTPStatus.NOT_FOUND,
            HTTPStatus.INTERNAL_SERVER_ERROR,
            HTTPStatus.BAD_GATEWAY
          ]

kinesis_client = boto3.client('kinesis')

def generate_logs():
  return {
    "timestamp" : datetime.utcnow().isoformat() + "Z",
    "service" : random.choice(services),
    "status" : random.choices(statuses, weights=[50, 20, 10, 5, 5, 5, 5])[0],
    "message" : fake.sentence(nb_words=6),
    "latency_ms" : round(random.uniform(50, 2000))
  }

def send_logs(log):
  response = kinesis_client.put_record(
    StreamName='log-stream',
    Data=log,
    PartitionKey='dummyKey'
  )
  print(f"Sent: {log} -> SequenceNumber: {response['SequenceNumber']}")

def main():
  for i in range(100):
    log = generate_logs()
    send_logs(json.dumps(log))
    time.sleep(1)

if __name__ == "__main__":
  main()