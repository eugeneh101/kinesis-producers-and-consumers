from abc import ABC, abstractmethod
from datetime import datetime
import json
from typing import Generic, TypeVar

import boto3


T = TypeVar("T")

class Source(ABC, Generic[T]):
    def __init__(
        self,
        stream_name: str,
    ):
        self.target_stream = stream_name
        self.kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)

    @abstractmethod
    async def checkpoint(self):
        """set a checkpoint"""
    
    @abstractmethod
    async def load_checkpoint(self):
        """load checkpoint for resuming progress"""

    @abstractmethod
    async def _run(self):
        """an async generator"""

    @abstractmethod
    async def handle_record(self, record):
        """convert record to desired format"""

    @abstractmethod
    async def get_write_time(self, data):
        """get the write time for the record"""

    async def run(self):
        # TODO: handle batches as well
        # TODO: occasionally checkpoint
        # TODO: on start, load from checkpoint
        async for data in self._run():
            write_time = await self.get_write_time(data)
            self.put_record(data, write_time)

    def put_record(self, value, write_time):
        record = {
            "Data": json.dumps(value).encode("utf-8"),
            "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
        }
        response = self.kinesis_client.put_records(
            StreamName=stream_name,
            Records=[record],
        )
        # TODO: instead of insert, maybe retry or log error?
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["FailedRecordCount"] == 0
        print(f'Published {len(response["Records"])} record(s) to "{stream_name}"')
