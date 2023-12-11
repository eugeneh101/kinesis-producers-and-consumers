import base64
import json
import os
import time
from datetime import datetime

import boto3


VERTEX_STREAM = os.environ["VERTEX_STREAM"]
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


def lambda_handler(event, context) -> None:
    now = datetime.utcnow()
    relevant_records = []
    records = event["Records"]
    for record in records:
        data_base64 = record["kinesis"]["data"]
        data = json.loads(base64.b64decode(data_base64).decode("utf-8"))
        if data["stream"] == VERTEX_STREAM:
            write_time = datetime.strptime(data["write_time"], "%Y-%m-%dT%H:%M:%S.%f")
            data["write_read_latency"] = (now - write_time).total_seconds()
            data["producer_type"] = "lambda"  # hard coded
            relevant_records.append(
                {
                    "Data": json.dumps(data).encode("utf-8"),
                    "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
                }
            )
    if relevant_records:
        response = kinesis_client.put_records(
            StreamName=VERTEX_STREAM,
            Records=relevant_records,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        if ENABLE_PRINT:
            print(f'Put {len(response["Records"])} record(s) in "{VERTEX_STREAM}"')
