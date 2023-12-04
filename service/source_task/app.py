import json
import os
import time
from datetime import datetime

import boto3
import numpy as np


SOURCE_STREAM = os.environ["SOURCE_STREAM"]
VERTEX_STREAMS = json.loads(os.environ["VERTEX_STREAMS"])
FREQUENCY_PER_MINUTE = json.loads(os.environ["FREQUENCY_PER_MINUTE"])
ENABLE_PRINT = json.loads(os.environ["ENABLE_PRINT"])
AWS_REGION = os.environ["AWS_REGION"]

kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)


def put_kinesis_records(
    stream_name: str,
    vertex_streams: list[str],
    frequency_per_minute: int,
    enable_print: bool,
) -> None:
    now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")
    records = []
    for i, vertex_stream in enumerate(vertex_streams, 1):
        record = {
            "Data": json.dumps(
                {
                    "stream": vertex_stream,
                    "value": np.random.poisson(lam=i * 100 / frequency_per_minute),
                    "write_time": now,
                }
            ).encode("utf-8"),
            "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
        }
        records.append(record)
    response = kinesis_client.put_records(
        StreamName=stream_name,
        Records=records,
    )
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["FailedRecordCount"] == 0
    if enable_print:
        print(f'Published {len(response["Records"])} record(s) to "{stream_name}"')


if __name__ == "__main__":
    while True:
        put_kinesis_records(
            stream_name=SOURCE_STREAM,
            vertex_streams=VERTEX_STREAMS,
            frequency_per_minute=FREQUENCY_PER_MINUTE,
            enable_print=ENABLE_PRINT,
        )
        time.sleep(60 / FREQUENCY_PER_MINUTE)
