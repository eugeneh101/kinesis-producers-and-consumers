from abc import abstractmethod
from typing import Generic, TypeVar

from .vertex import Vertex

T = TypeVar("T")


class Sink(Vertex[T, None], Generic[T]):
    async def get_storage(self):
        # TODO: implement a basic storage retrieval
        ...

    async def update_storage(self, new_storage):
        # TODO: implement a basic storage update
        ...

    @abstractmethod
    async def _handle_records(self, response: list[T]):
        ...

    async def run(self, shard_iter: str):
        response = self.kinesis_client.get_records(
            ShardIterator=shard_iter,
            Limit=self.max_batch_size,
        )
        await self._handle_record(response)
