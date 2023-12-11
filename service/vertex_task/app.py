import json
import os
import time
from datetime import datetime

import boto3
import pytz


STREAM_CHECKPOINTER = os.environ["STREAM_CHECKPOINTER"]
SOURCE_STREAM = os.environ["SOURCE_STREAM"]
VERTEX_STREAM = os.environ["VERTEX_STREAM"]
FREQUENCY_PER_MINUTE = json.loads(os.environ["FREQUENCY_PER_MINUTE"])
MAX_BATCH_SIZE = json.loads(os.environ["MAX_BATCH_SIZE"])
ENABLE_PRINT = json.loads(os.environ["ENABLE_PRINT"])
AWS_REGION = os.environ["AWS_REGION"]


kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)
dynamodb_table = dynamodb_resource.Table(STREAM_CHECKPOINTER)


def get_shard_iterator(
    source_stream: str, target_stream: str, enable_print: bool
) -> str:
    record = dynamodb_table.get_item(
        Key={  # hard coded keys
            "source_stream": source_stream,
            "target_stream": target_stream,
        },
        ConsistentRead=True,
    )
    response = kinesis_client.describe_stream(StreamName=source_stream)
    shards = response["StreamDescription"]["Shards"]
    assert len(shards) == 1, "Currently assuming only 1 shard"
    if "Item" in record:
        response = kinesis_client.get_shard_iterator(
            StreamName=source_stream,
            ShardId=shards[0]["ShardId"],  # assuming 1 shard
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber=record["Item"]["SequenceNumber"],
        )
        if enable_print:
            print(f"Starting from checkpoint using {record['Item']}")
    else:
        response = kinesis_client.get_shard_iterator(
            StreamName=source_stream,
            ShardId=shards[0]["ShardId"],  # assuming 1 shard
            ShardIteratorType="TRIM_HORIZON",
        )
        if enable_print:
            print(
                f'Starting from beginning for "{source_stream}" '
                f'to "{target_stream}"'
            )
    return response["ShardIterator"]


def get_and_put_kinesis_records(
    shard_iter: str,
    source_stream: str,
    target_stream: str,
    max_batch_size: int,
    enable_print: bool,
) -> str:
    response = kinesis_client.get_records(
        ShardIterator=shard_iter, Limit=max_batch_size
    )
    next_shard_iter = response["NextShardIterator"]
    records = response["Records"]
    exists_backpressure = max_batch_size == len(records)

    now = datetime.utcnow()
    relevant_records = []
    for record in records:
        data = json.loads(record["Data"].decode("utf-8"))
        if data["stream"] == target_stream:
            write_time = datetime.strptime(data["write_time"], "%Y-%m-%dT%H:%M:%S.%f")
            data["write_read_latency"] = (now - write_time).total_seconds()
            data["producer_type"] = "ecs"  # hard coded
            relevant_records.append(
                {
                    "Data": json.dumps(data).encode("utf-8"),
                    "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
                }
            )
    if relevant_records:
        response = kinesis_client.put_records(
            StreamName=target_stream,
            Records=relevant_records,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        approximate_arrival_timestamp = (
            record["ApproximateArrivalTimestamp"]
            .astimezone(pytz.utc)
            .strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
        )
        dynamodb_table.put_item(
            Item={
                "source_stream": source_stream,
                "target_stream": target_stream,
                "SequenceNumber": record["SequenceNumber"],
                "ApproximateArrivalTimestamp": approximate_arrival_timestamp,
            }
        )
        if enable_print:
            print(
                f'Put {len(response["Records"])} record(s) in "{target_stream}" '
                "and checkpointed"
            )
    return next_shard_iter, exists_backpressure


if __name__ == "__main__":
    shard_iter = get_shard_iterator(
        source_stream=SOURCE_STREAM,
        target_stream=VERTEX_STREAM,
        enable_print=ENABLE_PRINT,
    )
    while True:
        shard_iter, exists_backpressure = get_and_put_kinesis_records(
            shard_iter=shard_iter,
            source_stream=SOURCE_STREAM,
            target_stream=VERTEX_STREAM,
            max_batch_size=MAX_BATCH_SIZE,
            enable_print=ENABLE_PRINT,
        )
        while exists_backpressure:
            if ENABLE_PRINT:
                print(
                    f'There exists backpressure for "{SOURCE_STREAM}" to '
                    f'"{VERTEX_STREAM}". Catching up!'
                )
            shard_iter, exists_backpressure = get_and_put_kinesis_records(
                shard_iter=shard_iter,
                source_stream=SOURCE_STREAM,
                target_stream=VERTEX_STREAM,
                max_batch_size=MAX_BATCH_SIZE,
                enable_print=ENABLE_PRINT,
            )
        time.sleep(60 / FREQUENCY_PER_MINUTE)
