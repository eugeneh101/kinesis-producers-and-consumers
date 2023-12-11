import json

import cdk_ecr_deployment as ecr_deploy
from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_ecr_assets as ecr_assets,
    aws_iam as iam,
    aws_kinesis as kinesis,
    aws_lambda as _lambda,
    aws_logs as logs,
)
from constructs import Construct


def ecs_task_definition(
    stack: Stack,
    task_definition_name: str,
    task_directory: str,
    ecr_repo: ecr.Repository,
    role: iam.Role,
    env_vars: dict[str, str],
):
    """Mutates the stack"""
    task_asset = ecr_assets.DockerImageAsset(
        stack, f"EcrImage{task_definition_name}", directory=task_directory
    )  # uploads to `container-assets` ECR repo
    deploy_repo = ecr_deploy.ECRDeployment(  # upload to desired ECR repo
        stack,
        f"PushTaskImage{task_definition_name}",
        src=ecr_deploy.DockerImageName(task_asset.image_uri),
        dest=ecr_deploy.DockerImageName(ecr_repo.repository_uri),
    )
    log_group = logs.LogGroup(
        stack,
        f"TaskLogGroup{task_definition_name}",
        log_group_name=f"/ecs/{task_definition_name}",
        retention=logs.RetentionDays.ONE_MONTH,
        removal_policy=RemovalPolicy.DESTROY,
    )
    task_image = ecs.ContainerImage.from_ecr_repository(repository=ecr_repo)
    task_definition = ecs.TaskDefinition(
        stack,
        f"TaskDefinition{task_definition_name}",
        family=task_definition_name,
        compatibility=ecs.Compatibility.FARGATE,
        runtime_platform=ecs.RuntimePlatform(
            operating_system_family=ecs.OperatingSystemFamily.LINUX,
            cpu_architecture=ecs.CpuArchitecture.X86_64,
        ),
        cpu="256",  # 0.25 CPU
        memory_mib="512",  # 0.5 GB RAM
        # ephemeral_storage_gib=None,
        # volumes=None,
        execution_role=role,
        task_role=role,
    )
    container = task_definition.add_container(
        task_definition_name,
        image=task_image,
        logging=ecs.LogDrivers.aws_logs(
            stream_prefix="ecs",
            log_group=log_group,
            mode=ecs.AwsLogDriverMode.NON_BLOCKING,
        ),
        environment=env_vars,
    )
    # container.add_port_mappings(ecs.PortMapping(container_port=80))

    # make sure repo created before task definition
    task_definition.node.add_dependency(deploy_repo)

    return task_definition


class KinesisProducersAndConsumersStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.vpc = ec2.Vpc(
            self,
            "VPC",
            vpc_name=environment["VPC"],
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public-Subnet",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                # ec2.SubnetConfiguration(
                #     name="Private-Subnet",
                #     subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                #     cidr_mask=24,
                # ),
            ],
            availability_zones=[
                f"{environment['AWS_REGION']}{az}"
                for az in environment["AVAILABILITY_ZONES"]
            ],
            # nat_gateways=len(environment["AVAILABILITY_ZONES"]),
        )

        self.role = iam.Role(
            self,
            "EcsTaskExecutionAndLambdaRole",
            role_name=environment["IAM_ROLE_NAME"] + "-" + environment["AWS_REGION"],
            assumed_by=iam.CompositePrincipal(
                iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
                iam.ServicePrincipal("lambda.amazonaws.com"),
            ),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                ),  ### principle of least privileges later
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"  # write Cloudwatch logs
                ),
            ],
        )
        if environment["ECS_ENABLE_EXEC"]:
            self.role.add_to_policy(
                iam.PolicyStatement(
                    actions=[
                        "ssmmessages:CreateDataChannel",
                        "ssmmessages:OpenDataChannel",
                        "ssmmessages:OpenControlChannel",
                        "ssmmessages:CreateControlChannel",
                    ],
                    resources=["*"],  ### principle of least privileges later
                )
            )

        self.dynamodb_table = dynamodb.Table(
            self,
            "KinesisCheckpointTable",
            table_name=environment["STREAM_CHECKPOINTER"],
            partition_key=dynamodb.Attribute(
                name="source_stream",  # hard coded
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="target_stream",  # hard coded
                type=dynamodb.AttributeType.STRING,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.source_stream = kinesis.Stream(
            self,
            "SourceStream",
            stream_name=environment["SOURCE_STREAM"],
            stream_mode=kinesis.StreamMode.PROVISIONED,
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.UNENCRYPTED,  # needed for some reason
        )

        self.vertex_streams = {}
        for vertex_stream in environment["VERTEX_STREAMS"]:
            vertex_stream_instance = kinesis.Stream(
                self,
                f"VertexStream{vertex_stream}",
                stream_name=vertex_stream,
                stream_mode=kinesis.StreamMode.PROVISIONED,
                shard_count=1,
                retention_period=Duration.hours(24),
                encryption=kinesis.StreamEncryption.UNENCRYPTED,  # needed for some reason
            )
            self.vertex_streams[vertex_stream] = vertex_stream_instance

        self.sink_stream = kinesis.Stream(
            self,
            "SinkStream",
            stream_name=environment["SINK_STREAM"],
            stream_mode=kinesis.StreamMode.PROVISIONED,
            shard_count=1,
            retention_period=Duration.hours(24),
            encryption=kinesis.StreamEncryption.UNENCRYPTED,  # needed for some reason
        )

        self.ecs_cluster = ecs.Cluster(
            self,
            "EcsCluster",
            cluster_name=environment["ECS_CLUSTER_NAME"],
            vpc=self.vpc,
        )

        self.source_task_repo = ecr.Repository(
            self,
            "SourceTaskRepo",
            repository_name=environment["ECS_TASK_REPO_NAME_TEMPLATE"].format("source"),
            auto_delete_images=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.vertex_task_repo = ecr.Repository(
            self,
            "VertexTaskRepo",
            repository_name=environment["ECS_TASK_REPO_NAME_TEMPLATE"].format("vertex"),
            auto_delete_images=True,
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.sink_task_repo = ecr.Repository(
            self,
            "SinkTaskRepo",
            repository_name=environment["ECS_TASK_REPO_NAME_TEMPLATE"].format("sink"),
            auto_delete_images=True,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.vertex_lambdas = {}
        for vertex_stream in environment["VERTEX_STREAMS"]:
            vertex_lambda = _lambda.Function(
                self,
                f"Lambda{vertex_stream}",
                function_name=environment["LAMBDA_NAME_TEMPLATE"].format(vertex_stream),
                handler="handler.lambda_handler",
                memory_size=128,
                timeout=Duration.seconds(3),  # should be very fast
                runtime=_lambda.Runtime.PYTHON_3_9,
                environment={
                    "VERTEX_STREAM": vertex_stream,
                    "ENABLE_PRINT": json.dumps(environment["ENABLE_PRINT"]),
                    "AWSREGION": environment[
                        "AWS_REGION"
                    ],  # AWS doesn't allow "AWS_REGION" as environment variable
                },
                code=_lambda.Code.from_asset(
                    "lambda_code/vertex_lambda",
                    exclude=[".venv/*"],
                ),
                role=self.role,
                # retry_attempts=0,
                # vpc=self.vpc,
                # vpc_subnets=...,
                # security_groups=...,
                # log_retention=logs.RetentionDays.ONE_WEEK,
                # log_retention_role=role,
            )
            self.vertex_lambdas[vertex_stream] = vertex_lambda
            log_group = logs.LogGroup(
                self,
                f"LogGroup{vertex_stream}",
                log_group_name="/aws/lambda/{}".format(
                    environment["LAMBDA_NAME_TEMPLATE"].format(vertex_stream)
                ),
                retention=logs.RetentionDays.ONE_WEEK,  # hard coded
                removal_policy=RemovalPolicy.DESTROY,
            )
            # make sure log group created before Lambda, so Lambda does not create
            # log group by itself
            vertex_lambda.node.add_dependency(log_group)
        self.sink_lambda = _lambda.Function(
            self,
            f"LambdaSinkStream",
            function_name=environment["LAMBDA_NAME_TEMPLATE"].format(
                environment["SINK_STREAM"]
            ),
            handler="handler.lambda_handler",
            memory_size=128,
            timeout=Duration.seconds(3),  # should be very fast
            runtime=_lambda.Runtime.PYTHON_3_9,
            environment={
                "SINK_STREAM": environment["SINK_STREAM"],
                "ENABLE_PRINT": json.dumps(environment["ENABLE_PRINT"]),
                "AWSREGION": environment[
                    "AWS_REGION"
                ],  # AWS doesn't allow "AWS_REGION" as environment variable
            },
            code=_lambda.Code.from_asset(
                "lambda_code/sink_lambda",
                exclude=[".venv/*"],
            ),
            role=self.role,
        )
        log_group = logs.LogGroup(
            self,
            f"LogGroupSinkStream",
            log_group_name="/aws/lambda/{}".format(
                environment["LAMBDA_NAME_TEMPLATE"].format(environment["SINK_STREAM"])
            ),
            retention=logs.RetentionDays.ONE_WEEK,  # hard coded
            removal_policy=RemovalPolicy.DESTROY,
        )
        # make sure log group created before Lambda, so Lambda does not create
        # log group by itself
        self.sink_lambda.node.add_dependency(log_group)

        # connecting AWS resources together
        self.dynamodb_table.grant_read_write_data(grantee=self.role)
        self.source_stream.grant_read_write(grantee=self.role)
        for vertex_stream_instance in self.vertex_streams.values():
            vertex_stream_instance.grant_read_write(grantee=self.role)
        self.sink_stream.grant_read_write(grantee=self.role)

        self.source_task_definition = ecs_task_definition(
            stack=self,
            task_definition_name=environment[
                "ECS_TASK_DEFINITION_NAME_TEMPLATE"
            ].format("source"),
            task_directory="service/source_task",  # hard coded
            ecr_repo=self.source_task_repo,
            role=self.role,
            env_vars={
                "SOURCE_STREAM": environment["SOURCE_STREAM"],
                "VERTEX_STREAMS": json.dumps(environment["VERTEX_STREAMS"]),
                "FREQUENCY_PER_MINUTE": json.dumps(60),  # hard coded
                "ENABLE_PRINT": json.dumps(environment["ENABLE_PRINT"]),
                "AWS_REGION": environment["AWS_REGION"],
            },
        )
        self.vertex_task_definitions = {}
        for vertex_stream in environment["VERTEX_STREAMS"]:
            vertex_task_definition = ecs_task_definition(
                stack=self,
                task_definition_name=environment[
                    "ECS_TASK_DEFINITION_NAME_TEMPLATE"
                ].format(vertex_stream),
                task_directory="service/vertex_task",  # hard coded
                ecr_repo=self.vertex_task_repo,
                role=self.role,
                env_vars={
                    "STREAM_CHECKPOINTER": environment["STREAM_CHECKPOINTER"],
                    "SOURCE_STREAM": environment["SOURCE_STREAM"],
                    "VERTEX_STREAM": vertex_stream,
                    "FREQUENCY_PER_MINUTE": json.dumps(60),  # hard coded
                    "MAX_BATCH_SIZE": json.dumps(environment["MAX_BATCH_SIZE"]),
                    "ENABLE_PRINT": json.dumps(environment["ENABLE_PRINT"]),
                    "AWS_REGION": environment["AWS_REGION"],
                },
            )
            self.vertex_task_definitions[vertex_stream] = vertex_task_definition
        self.sink_task_definition = ecs_task_definition(
            stack=self,
            task_definition_name=environment[
                "ECS_TASK_DEFINITION_NAME_TEMPLATE"
            ].format("sink"),
            task_directory="service/sink_task",  # hard coded
            ecr_repo=self.sink_task_repo,
            role=self.role,
            env_vars={
                "STREAM_CHECKPOINTER": environment["STREAM_CHECKPOINTER"],
                "VERTEX_STREAMS": json.dumps(environment["VERTEX_STREAMS"]),
                "SINK_STREAM": environment["SINK_STREAM"],
                "FREQUENCY_PER_MINUTE": json.dumps(len(environment["VERTEX_STREAMS"])),
                "MAX_BATCH_SIZE": json.dumps(environment["MAX_BATCH_SIZE"]),
                "ENABLE_PRINT": json.dumps(environment["ENABLE_PRINT"]),
                "AWS_REGION": environment["AWS_REGION"],
            },
        )

        # if ECS fails to deploy successfully, the Cloudformation stack gets stuck for up to 3 hours
        if environment["ECS_ACTIVATE_SERVICES"]:
            self.source_service = ecs.FargateService(
                self,
                "SourceService",
                service_name=environment["ECS_SERVICE_NAME_TEMPLATE"].format("source"),
                cluster=self.ecs_cluster,
                task_definition=self.source_task_definition,
                desired_count=1,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                assign_public_ip=True,  # seems to need to be True if using Public subnet
                enable_execute_command=environment["ECS_ENABLE_EXEC"],
                # security_groups=[],
            )
            self.vertex_services = {}
            for vertex_stream in environment["VERTEX_STREAMS"]:
                vertex_service = ecs.FargateService(
                    self,
                    f"VertexService{vertex_stream}",
                    service_name=environment["ECS_SERVICE_NAME_TEMPLATE"].format(
                        vertex_stream
                    ),
                    cluster=self.ecs_cluster,
                    task_definition=self.vertex_task_definitions[vertex_stream],
                    desired_count=1,
                    vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                    assign_public_ip=True,  # seems to need to be True if using Public subnet
                    enable_execute_command=environment["ECS_ENABLE_EXEC"],
                    # security_groups=[],
                )
                self.vertex_services[vertex_stream] = vertex_service
            self.sink_service = ecs.FargateService(
                self,
                "SinkService",
                service_name=environment["ECS_SERVICE_NAME_TEMPLATE"].format("sink"),
                cluster=self.ecs_cluster,
                task_definition=self.sink_task_definition,
                desired_count=1,
                vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
                assign_public_ip=True,  # seems to need to be True if using Public subnet
                enable_execute_command=environment["ECS_ENABLE_EXEC"],
                # security_groups=[],
            )

        if environment["CONNECT_LAMBDAS_TO_KINESIS"]:
            self.kinesis_efos = {}
            self.lambda_event_source_mappings = {}
            for vertex_stream in environment["VERTEX_STREAMS"]:
                vertex_lambda = self.vertex_lambdas[vertex_stream]
                vertex_lambda.role.add_to_policy(
                    iam.PolicyStatement(
                        actions=[
                            "kinesis:ListStreams",
                            "kinesis:PutRecord",
                            "kinesis:SubscribeToShard",
                            "kinesis:DescribeStreamSummary",
                            "kinesis:ListShards",
                            "kinesis:PutRecords",
                            "kinesis:GetShardIterator",
                            "kinesis:GetRecords",
                            "kinesis:DescribeStream",
                        ],
                        resources=["*"],  ### principle of least privileges later
                    )
                )
                kinesis_efo = kinesis.CfnStreamConsumer(
                    self,
                    f"EfoFromSourceTo{vertex_stream}Lambda",
                    consumer_name=f"efo-from-source-stream-to-{vertex_stream}-lambda",
                    stream_arn=self.source_stream.stream_arn,
                )
                self.kinesis_efos[
                    (environment["SOURCE_STREAM"], vertex_stream)
                ] = kinesis_efo  # source-target
                lambda_event_source_mapping = _lambda.EventSourceMapping(
                    self,
                    f"LambdaEventSourceMappingFor{vertex_stream}",
                    target=vertex_lambda,
                    batch_size=environment["MAX_BATCH_SIZE"],
                    event_source_arn=kinesis_efo.attr_consumer_arn,
                    # filters=None,
                    max_batching_window=Duration.seconds(0),  # instantaneous
                    starting_position=_lambda.StartingPosition.LATEST,
                    # on_failure=None,
                    # parallelization_factor=None,
                    # retry_attempts=None,
                )
                self.lambda_event_source_mappings[
                    (environment["SOURCE_STREAM"], vertex_stream)
                ] = lambda_event_source_mapping  # source-target

                vertex_stream_instance = self.vertex_streams[vertex_stream]
                kinesis_efo = kinesis.CfnStreamConsumer(
                    self,
                    f"EfoFrom{vertex_stream}ToSinkStreamLambda",
                    consumer_name=f"efo-from-{vertex_stream}-to-sink-lambda",
                    stream_arn=vertex_stream_instance.stream_arn,
                )
                self.kinesis_efos[
                    (vertex_stream, environment["SINK_STREAM"])
                ] = kinesis_efo  # source-target
                lambda_event_source_mapping = _lambda.EventSourceMapping(
                    self,
                    f"LambdaEventSourceMappingForSinkStreamFrom{vertex_stream}",
                    target=self.sink_lambda,
                    batch_size=environment["MAX_BATCH_SIZE"],
                    event_source_arn=kinesis_efo.attr_consumer_arn,
                    # filters=None,
                    max_batching_window=Duration.seconds(60),  # gather once per minute
                    # tumbling_window is better than max_batching_window, but needs special return key
                    # tumbling_window=Duration.seconds(60),
                    starting_position=_lambda.StartingPosition.LATEST,
                    # on_failure=None,
                    # parallelization_factor=None,
                    # retry_attempts=None,
                )
                self.lambda_event_source_mappings[
                    (vertex_stream, environment["SINK_STREAM"])
                ] = lambda_event_source_mapping  # source-target
            if environment["ECS_ACTIVATE_SERVICES"]:
                # make sure Lambda attached to EFO before ECS service publish into Kinesis
                for (
                    lambda_event_source_mapping
                ) in self.lambda_event_source_mappings.values():
                    self.source_service.node.add_dependency(lambda_event_source_mapping)
