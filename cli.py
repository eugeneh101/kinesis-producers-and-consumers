from service.source import Source

# This is always a Source because a Vertex is a Source and a sink is a Vertex


# Imagine a user provides a python file with their Source implementation
# We would need to deploy that as a lambda
# Add it as a SNS/EventBridge trigger


def deploy_worker(worker: Source):
    """deploys a new worker"""


def update_worker(worker: Source):
    """updates a worker to this updated implementation"""
    # TODO: should this be an initialized class, or should the init args be provided via config?


def stop_worker(name: str):
    """stops a worker"""


def get_status(name: str):
    """gets some status of a worker"""
    # TODO: maybe a worker records metadata in a dynamo field
    # for example... number of records consumed, current offset, etc.
