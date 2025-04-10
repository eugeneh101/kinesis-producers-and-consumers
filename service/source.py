import asyncio
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from enum import Enum, auto
import json
from typing import Any, Generic, TypeVar

import boto3
from pydantic import BaseModel

T = TypeVar("T")

class Status(Enum):
    sleeping = auto()
    running = auto()
    error = auto()
    selfdestructed = auto()


class Stats(BaseModel):
    ingest_count: int = 0
    publish_count: int = 0
    status: Status = Status.sleeping


class Source(ABC, Generic[T]):
    name: str
    stats: Stats

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
        self.stats = Stats()

    async def get_storage(self):
        """this will utilize a key in dyanmo like `{name}-storage`"""
        # TODO: implement a basic storage retrieval
        ...

    async def update_storage(self, new_storage: dict[Any, Any]):
        # TODO: implement a basic storage update
        ...

    async def get_stats(self):
        """pulls in the current status in the case that the lambda went cold or something"""
        raise NotImplementedError()

    async def update_stats(self, new_status: Stats):
        """this will utilize a key in dyanmo like `{name}-stats`"""
        raise NotImplementedError()

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
        stats = await self.get_stats()
        stats.status = Status.running
        await self.update_stats(stats)

        async for data in self._run():
            self.put_record(data)
            stats.publish_count += len(data)
            asyncio.create_task(self.update_stats(stats))

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
