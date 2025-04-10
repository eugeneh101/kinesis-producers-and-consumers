from abc import abstractmethod
from enum import Enum, auto
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from .vertex import Vertex

T = TypeVar("T")


class Sink(Vertex[T, None], Generic[T]):


    @abstractmethod
    async def _handle_records(self, response: list[T]):
        ...

    async def run(self, shard_iter: str):
        response = self.kinesis_client.get_records(
            ShardIterator=shard_iter,
            Limit=self.max_batch_size,
        )
        await self._handle_record(response)
