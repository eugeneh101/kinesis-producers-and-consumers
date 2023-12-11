import json
import os
import time

import boto3
import pytz


STREAM_CHECKPOINTER = os.environ["STREAM_CHECKPOINTER"]
VERTEX_STREAMS = json.loads(os.environ["VERTEX_STREAMS"])
SINK_STREAM = os.environ["SINK_STREAM"]
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


def get_and_reduce_and_put_kinesis_records(
    shard_iter: str,
    source_stream: str,
    target_stream: str,
    max_batch_size: int,
    enable_print: bool,
) -> tuple[str, bool]:
    response = kinesis_client.get_records(
        ShardIterator=shard_iter, Limit=max_batch_size
    )
    next_shard_iter = response["NextShardIterator"]
    records = response["Records"]
    exists_backpressure = max_batch_size == len(records)

    summed_value = 0
    summed_write_read_latency = 0
    count = 0
    for record in records:
        data = json.loads(record["Data"].decode("utf-8"))
        stream = data["stream"]
        assert stream == source_stream, f'Expected "{source_stream} but got "{stream}"'
        if data["producer_type"] == "ecs":  # hard coded
            summed_value += data["value"]
            summed_write_read_latency += data["write_read_latency"]
            count += 1
    if count:
        reduced_record = {
            "stream": source_stream,
            "summed_value": summed_value,
            "count": count,
            "average_write_read_latency": summed_write_read_latency / count,
            "producer_type": "ecs"  # hard coded
        }
        response = kinesis_client.put_records(
            StreamName=target_stream,
            Records=[
                {
                    "Data": json.dumps(reduced_record).encode("utf-8"),
                    "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
                }
            ],
        )
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
                f'Reduced {count} records from "{source_stream}", '
                f'put {len(response["Records"])} record(s) in "{target_stream}", '
                "and checkpointed"
            )
    return next_shard_iter, exists_backpressure


if __name__ == "__main__":
    stream_iterators = {}
    for vertex_stream in VERTEX_STREAMS:
        stream_iterators[vertex_stream] = get_shard_iterator(
            source_stream=vertex_stream,
            target_stream=SINK_STREAM,
            enable_print=ENABLE_PRINT,
        )
    while True:
        for vertex_stream in VERTEX_STREAMS:
            (
                stream_iterators[vertex_stream],
                exists_backpressure,
            ) = get_and_reduce_and_put_kinesis_records(
                shard_iter=stream_iterators[vertex_stream],
                source_stream=vertex_stream,
                target_stream=SINK_STREAM,
                max_batch_size=MAX_BATCH_SIZE,
                enable_print=ENABLE_PRINT,
            )
            while exists_backpressure:
                if ENABLE_PRINT:
                    print(
                        f'There exists backpressure for "{vertex_stream}" to '
                        f'"{SINK_STREAM}". Catching up!'
                    )
                (
                    stream_iterators[vertex_stream],
                    exists_backpressure,
                ) = get_and_reduce_and_put_kinesis_records(
                    shard_iter=stream_iterators[vertex_stream],
                    source_stream=vertex_stream,
                    target_stream=SINK_STREAM,
                    max_batch_size=MAX_BATCH_SIZE,
                    enable_print=ENABLE_PRINT,
                )
            time.sleep(60 / FREQUENCY_PER_MINUTE)
