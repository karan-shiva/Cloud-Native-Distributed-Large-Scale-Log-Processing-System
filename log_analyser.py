import boto3
import json

kinesis = boto3.client('kinesis')
STREAM_NAME = "log-stream"

def get_shard_IDs():
  response = kinesis.describe_stream(
    StreamName = STREAM_NAME
  )
  shards = response['StreamDescription']['Shards']
  return [shard['ShardId'] for shard in shards]

def get_shard_iterator(shard_ID):
  response = kinesis.get_shard_iterator(
    StreamName = STREAM_NAME,
    ShardId = shard_ID,
    ShardIteratorType='TRIM_HORIZON'
  )
  return response['ShardIterator']

def get_data(shard_iterator):
  response = kinesis.get_records(
    ShardIterator = shard_iterator
  )
  records = response['Records']
  return [json.loads(record['Data']) for record in records]


def read_logs():
  shard_IDs = get_shard_IDs()
  for shard_ID in shard_IDs:
    shard_iterator = get_shard_iterator(shard_ID)
    print(json.dumps(get_data(shard_iterator), indent=2))


if __name__ == "__main__":
  read_logs()