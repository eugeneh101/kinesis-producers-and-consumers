import json
import os
import time

import boto3
import pytz


STREAM_CHECKPOINTER = os.environ["STREAM_CHECKPOINTER"]
SOURCE_STREAM = os.environ["SOURCE_STREAM"]
VERTEX_STREAM = os.environ["VERTEX_STREAM"]
FREQUENCY_PER_MINUTE = json.loads(os.environ["FREQUENCY_PER_MINUTE"])
AWS_REGION = os.environ["AWS_REGION"]


kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
dynamodb_resource = boto3.resource("dynamodb", region_name=AWS_REGION)
dynamodb_table = dynamodb_resource.Table(STREAM_CHECKPOINTER)


def get_shard_iterator(stream_name: str, filter_name: str) -> str:
    response = kinesis_client.describe_stream(StreamName=stream_name)
    details = response["StreamDescription"]
    assert len(details["Shards"]) == 1, "Currently assuming only 1 shard"

    record = dynamodb_table.get_item(
        Key={"stream_name": stream_name, "filter_name": filter_name}
    )  # hard coded keys
    if "Item" in record:
        print(f"starting from checkpoint using {record['Item']}")
        response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=details["Shards"][0]["ShardId"],  # assuming 1 shard
            ShardIteratorType="AFTER_SEQUENCE_NUMBER",
            StartingSequenceNumber=record["Item"]["SequenceNumber"],
        )
    else:
        print("starting from beginning")
        response = kinesis_client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=details["Shards"][0]["ShardId"],  # assuming 1 shard
            ShardIteratorType="TRIM_HORIZON",
        )
    return response["ShardIterator"]


def get_and_put_kinesis_records(
    shard_iter: str, stream_name: str, filter_name: str
) -> str:
    response = kinesis_client.get_records(
        ShardIterator=shard_iter, Limit=10  # hard coded limit
    )
    next_shard_iter = response["NextShardIterator"]
    records = response["Records"]
    relevant_records = []
    for record in records:
        stream = json.loads(record["Data"].decode("utf-8"))["stream"]
        if stream == filter_name:
            relevant_records.append(
                {
                    "Data": record["Data"],
                    "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
                }
            )
    if relevant_records:
        response = kinesis_client.put_records(
            StreamName=filter_name,
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
                "stream_name": stream_name,
                "filter_name": filter_name,
                "SequenceNumber": record["SequenceNumber"],
                "ApproximateArrivalTimestamp": approximate_arrival_timestamp,
            }
        )
    return next_shard_iter


if __name__ == "__main__":
    shard_iter = get_shard_iterator(
        stream_name=SOURCE_STREAM, filter_name=VERTEX_STREAM
    )
    while True:
        shard_iter = get_and_put_kinesis_records(
            shard_iter=shard_iter, stream_name=SOURCE_STREAM, filter_name=VERTEX_STREAM
        )
        time.sleep(60 / FREQUENCY_PER_MINUTE)
