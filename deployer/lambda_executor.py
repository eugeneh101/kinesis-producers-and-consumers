from abc import ABC, abstractmethod
import requests

class LambdaExecutor(ABC):
    """THESE RUN ON THE LAMBDA"""
    """These are the utilities for a running lambda to control its execution"""
    backend_uri: str
    API_KEY: str

    def __init__(self, *args, backend_uri, api_key, **kwargs):
        self.backend_uri = backend_uri
        self.API_KEY = api_key
        super().__init__(*args, **kwargs)

    def _send_to_backend(self, msg, value={}):
        requests.post(self.backend_uri, json={
            'msg': msg,
            'value': value,
        },
        headers={
            'X-LAMBDA-API-KEY': self.API_KEY,
        })

    def add_log(self, log, log_level):
        """sends a log to the backend to add to a log store, so a user can get status updates from the lambda"""
        self._send_to_backend("LOG", {'log': log, 'level': log_level})

    def stop(self):
        """Stop actually temporarily disconnects the labmda from SNS"""
        self._send_to_backend("STOP", {})

    def pause(self):
        """pause continues to consume from SNS, but updates the status"""
        self._send_to_backend("PAUSE", {})

    def resume(self):
        """updates the status to running and reconnects the lambda to SNS"""
        self._send_to_backend("RESUME")

    def unpause(self):
        """updates the status to running"""
        self._send_to_backend("UNPAUSE")

    def selfdestruct(self):
        """Kill the lambda FOREVER!"""
        self._send_to_backend("SELF_DESTRUCT")

    @abstractmethod
    def run(self, message):
        """run the logic of the lambda"""
