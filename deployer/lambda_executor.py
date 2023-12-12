class LambdaExecutor:
    """THESE RUN ON THE LAMBDA""""
    """These are the utilities for a running lambda to control its execution"""
    def add_log(self, log, log_level):
        """sends a log to the backend to add to a log store, so a user can get status updates from the lambda"""
        ...

    def stop(self):
        """Stop actually temporarily disconnects the labmda from SNS"""
        ...

    def pause(self):
        """pause continues to consume from SNS, but updates the status"""
        ...

    def resume(self):
        """updates the status to running and reconnects the lambda to SNS"""
        ...

    def unpause(self):
        """updates the status to running"""
        ...

    def selfdestruct(self):
        """Kill the lambda FOREVER!"""
        ...
