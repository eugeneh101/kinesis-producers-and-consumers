import base64
import json
import os
import time
from datetime import datetime

import boto3


SINK_STREAM = os.environ["SINK_STREAM"]
ENABLE_PRINT = json.loads(os.environ["ENABLE_PRINT"])
AWS_REGION = os.environ[
    "AWSREGION"
]  # AWS doesn't allow "AWS_REGION" as environment variable

kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
### how to deal with backfill record?
### LATEST, not TRIM_HORIZON to prevent duplicate ingestion
### can apply filter condition to Lambda
### retry sent to DLQ
### don't need checkpointer? or checkpoint for observability?
### tumbling_window is better than max_batching_window, but needs special return key


def lambda_handler(event, context) -> None:
    # print(event)  # if want to see "state" from tumbling_window
    summed_value = 0
    summed_write_read_latency = 0
    count = 0
    records = event["Records"]
    source_streams = set()
    for record in records:
        data_base64 = record["kinesis"]["data"]
        data = json.loads(base64.b64decode(data_base64).decode("utf-8"))
        if data["producer_type"] == "lambda":
            summed_value += data["value"]
            summed_write_read_latency += data["write_read_latency"]
            count += 1
            source_streams.add(data["stream"])
    if count:
        assert (
            len(source_streams) == 1
        ), f"Expected 1 source stream but got {source_streams}"
        source_stream = source_streams.pop()
        reduced_record = {
            "stream": source_stream,
            "summed_value": summed_value,
            "count": count,
            "average_write_read_latency": summed_write_read_latency / count,
            "producer_type": "lambda",  # hard coded
        }
        response = kinesis_client.put_records(
            StreamName=SINK_STREAM,
            Records=[
                {
                    "Data": json.dumps(reduced_record).encode("utf-8"),
                    "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
                }
            ],
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        if ENABLE_PRINT:
            print(
                f'Reduced {count} records from "{source_stream}" and '
                f'put {len(response["Records"])} record(s) in "{SINK_STREAM}"'
            )
