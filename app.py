import aws_cdk as cdk

from kinesis_producers_and_consumers import KinesisProducersAndConsumersStack

app = cdk.App()
environment = app.node.try_get_context("environment")
KinesisProducersAndConsumersStack(
    app,
    "KinesisProducersAndConsumersStack",
    env=cdk.Environment(region=environment["AWS_REGION"]),
    environment=environment,
)
app.synth()
