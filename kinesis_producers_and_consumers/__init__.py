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
    ecr_deploy.ECRDeployment(  # upload to desired ECR repo
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
            "EcsTaskExecutionRole",
            role_name=environment["IAM_ROLE_NAME"] + "-" + environment["AWS_REGION"],
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonECSTaskExecutionRolePolicy"
                ),  ### later principle of least privileges
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
                    resources=["*"],
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
                    "FREQUENCY_PER_MINUTE": json.dumps(30),  # hard coded
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
