from dataclasses import dataclass

import pickle
import requests

from service.sink import Sink
# from service.custom_sinks import SwapSink
# from empyrealSDK import deploy_or_update_worker # type: ignore


def deploy_or_update_worker(worker):
    # UNSAFE!!!!
    pickled_worker = pickle.dumps(worker)
    requests.post("https://upload.xyz/new_worker", content=pickled_worker)


@dataclass
class Record:
    """This would be defined by the stream they are consuming from"""
    a: int
    b: int
SwapSink = Sink[Record]


class MySink(SwapSink):
    async def _handle_records(self, records: list[Record]):
        for record in records:
            my_storage = await self.get_storage()
            a = record['a']
            my_storage[a] += 1
            self.update(my_storage)

            requests.post(f"http://myapi.xyz/new_record?value={a}")


if __name__ == "__main__":
    my_sink = MySink(name="my-record-handler")
    deploy_or_update_worker(my_sink)
