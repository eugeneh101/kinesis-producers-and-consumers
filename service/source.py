from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from datetime import datetime
import json
from typing import Generic, TypeVar

import boto3

T = TypeVar("T")


class Source(ABC, Generic[T]):
    name: str
    def __init__(
        self,
        name: str,
        stream_name: str,
        aws_region="us-east-2",
    ):
        """The name is used to uniquely identify the worker"""
        self.name = name
        self.target_stream = stream_name
        self.kinesis_client = boto3.client("kinesis", region_name=aws_region)

    @abstractmethod
    async def checkpoint(self):
        """set a checkpoint"""
    
    @abstractmethod
    async def load_checkpoint(self):
        """load checkpoint for resuming progress"""

    @abstractmethod
    async def _run(self) -> AsyncGenerator[list[T]]:
        """an async generator"""

    async def run(self):
        # TODO: handle batches as well
        # TODO: occasionally checkpoint
        # TODO: on start, load from checkpoint
        async for data in self._run():
            self.put_record(data)

    def put_record(self, value: list[T]):
        # TODO: put records
        record = {
            "Data": json.dumps(value).encode("utf-8"),
            "PartitionKey": "doesn't matter if only 1 shard",  # hard coded
        }
        response = self.kinesis_client.put_records(
            StreamName=self.stream_name,
            Records=[record],
        )
        # TODO: instead of assert, maybe retry or log error?
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        assert response["FailedRecordCount"] == 0
        print(f'Published {len(response["Records"])} record(s) to "{self.stream_name}"')
