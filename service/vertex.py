from abc import abstractmethod
import json
from typing import Generic, Optional, TypeVar

from .source import Source

T = TypeVar("T")
U = TypeVar("U")


class Vertex(Source[T], Generic[T, U]):
    def __init__(
        self,
        source_stream: str,
        target_stream: str,
        max_batch_size: int = 100,
    ):
        super().__init__(target_stream)
        self.source_stream = source_stream
        self.max_batch_size = max_batch_size

    @abstractmethod
    async def handle_record(self, record) -> Optional[T]:
        """takes a record from the consumed stream and returns an Optional output record"""
        # NOTE: if it returns None the record will not be published downstream

    def get_shard_iterator(
        self,
        enable_print: bool = True,
    ) -> str:
        """Which shard are you in, and where is your latest checkpoint in that shard"""

        # TODO: clean this up
        record = self.dynamodb_table.get_item(
            Key={  # hard coded keys
                "source_stream": self.source_stream,
                "target_stream": self.target_stream,
            },
            ConsistentRead=True,
        )
        response = self.kinesis_client.describe_stream(StreamName=self.source_stream)
        shards = response["StreamDescription"]["Shards"]
        assert len(shards) == 1, "Currently assuming only 1 shard"
        if "Item" in record:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.source_stream,
                ShardId=shards[0]["ShardId"],  # assuming 1 shard
                ShardIteratorType="AFTER_SEQUENCE_NUMBER",
                StartingSequenceNumber=record["Item"]["SequenceNumber"],
            )
            if enable_print:
                print(f"Starting from checkpoint using {record['Item']}")
        else:
            response = self.kinesis_client.get_shard_iterator(
                StreamName=self.source_stream,
                ShardId=shards[0]["ShardId"],  # assuming 1 shard
                ShardIteratorType="TRIM_HORIZON",
            )
            if enable_print:
                print(
                    f'Starting from beginning for "{self.source_stream}" '
                    f'to "{self.target_stream}"'
                )
        return response["ShardIterator"]

    async def run(self, shard_iter: str, enable_print: bool = True):
        response = self.kinesis_client.get_records(
            ShardIterator=shard_iter,
            Limit=self.max_batch_size,
        )
        next_shard_iter = response["NextShardIterator"]
        records = response["Records"]
        exists_backpressure = self.max_batch_size == len(records)

        relevant_records = []
        for record in records:
            data = json.loads(record["Data"].decode("utf-8"))
            result = await self.handle_data(data)
            if result:
                relevant_records.append(result)
        if relevant_records:
            response = self.kinesis_client.put_records(
                StreamName=self.target_stream,
                Records=relevant_records,
            )
            assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
            self.dynamodb_table.put_item(
                Item={
                    "source_stream": self.source_stream,
                    "target_stream": self.target_stream,
                    "SequenceNumber": record["SequenceNumber"],
                }
            )
            if enable_print:
                print(
                    f'Put {len(response["Records"])} record(s) in "{self.target_stream}" '
                    "and checkpointed"
                )
        return next_shard_iter, exists_backpressure
