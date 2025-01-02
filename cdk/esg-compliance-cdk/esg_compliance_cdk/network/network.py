import os
from constructs import Construct 
import aws_cdk as cdk
from aws_cdk import CfnOutput
from aws_cdk import (
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_ecr as ecr,
    aws_efs as efs,
    aws_elasticloadbalancingv2 as elbv2,
    aws_elasticloadbalancingv2_actions as actions,
    aws_elasticloadbalancingv2_targets as targets,
    aws_events as events,
    aws_events_targets as targets,
    aws_ecr_assets as assets,
    aws_iam as iam,
    aws_logs as logs,
    aws_servicediscovery as sd,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_sns as sns,
    aws_route53 as route53,
    aws_cognito as cognito,
    aws_secretsmanager as secrets,
    aws_certificatemanager as acm,
    aws_s3 as s3
)


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..",".."))
streamlit_app_dir = os.path.join(PROJECT_ROOT, "streamlit")


class CoreNetwork(Construct):
    
    def __init__(self, scope: Construct, id:str, prefix,
                 user_pool:cognito.IUserPool,
                 user_pool_client:cognito.IUserPoolClient,
                 user_pool_domain:cognito.UserPoolDomain,
                 identity_pool:cognito.CfnIdentityPool,
                 secret: secrets.ISecret,
                 report_bucket: s3.IBucket,
                 gradings_bucket: s3.IBucket,
                 **kwargs):
        super().__init__(scope, id)
        
        acmSecret = secrets.Secret.from_secret_name_v2(
            self,
            'acmSecret',
            'acmSecret'
        )
        
        certificate = acm.Certificate.from_certificate_arn(
            self,
            f"{prefix}-DomainCertificate",
            certificate_arn=acmSecret.secret_value_from_json("certificate_arn").to_string()
        )
        
        vpc = ec2.Vpc(
            self, 
            f"{prefix}-Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16")
        )
        
        ecs_security_group = ec2.SecurityGroup(
            self,
            f"{prefix}-ECSSecurityGroup",
            vpc=vpc,
            security_group_name=f"{prefix}-stl-ecs-sg"  
        )
        
        alb_security_group = ec2.SecurityGroup(
            self,
            f"{prefix}-ALBSecurityGroup",
            vpc=vpc,
            security_group_name=f"{prefix}-stl-alb-sg"
        )
        
        ecs_security_group.add_ingress_rule(
            peer=alb_security_group,
            connection=ec2.Port.tcp(8501),
            description="ALB traffic"
        )
        
        cluster = ecs.Cluster(
            self,
            f"{prefix}-Cluster",
            vpc=vpc,
            enable_fargate_capacity_providers=True
        )
        
        
        load_balancer = elbv2.ApplicationLoadBalancer(
            self,
            f"{prefix}-alb",
            vpc=vpc,
            internet_facing=True,
            load_balancer_name=f"{prefix}-stl-alb",
            security_group=alb_security_group,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC)
        )
        
        fargate_task_definition = ecs.FargateTaskDefinition(
            self,
            f"{prefix}-TaskDefinition",
            cpu=1024,
            memory_limit_mib=8192,
        )


        # asset = assets.DockerImageAsset(self, "MyBuildImage",
        #     directory=streamlit_app_dir,
        #     platform=assets.Platform.LINUX_ARM64
        # )


        streamlit_repository = ecr.Repository(self, "StreamlitAppRepo")

        # # Define the Docker image asset
        # docker_image = ecr_assets.DockerImageAsset(self, "StreamlitDockerImage",
        #    directory="./streamlit")  # Path to the directory containing your Dockerfile

        # # Grant permissions for pushing the image to ECR
        # repository.add_lifecycle_rule(max_image_count=1)

        # # Push the Docker image to ECR
        # repository.add_to_resource_policy(
        #     ecr.PolicyStatement(
        #         actions=["ecr:PutImage"],
        #         effect=core.Effect.ALLOW,
        #         principals=[core.ArnPrincipal("*")]
        #     )
        # )

        image = ecs.ContainerImage.from_asset(
            directory=streamlit_app_dir,
            platform=assets.Platform.LINUX_AMD64
            )
        
        
        fargate_task_definition.add_container(
            f"{prefix}-WebContainer",
            image=image,
            port_mappings=[
                ecs.PortMapping(
                    container_port=8501,
                    protocol=ecs.Protocol.TCP
                )
            ],
            environment={
                "COGNITO_USER_POOL_ID": user_pool.user_pool_id,
                "COGNITO_APP_CLIENT_ID": user_pool_client.user_pool_client_id,
                "COGNITO_APP_CLIENT_SECRET": user_pool_client.user_pool_client_secret.to_string(),
                "COGNITO_DOMAIN": user_pool_domain.base_url(),
                "COGNITO_REDIRECT_URI": "INSERT_DOMAIN_URL",
                "COGNITO_IDENTITY_POOL_ID": identity_pool.ref,
                "AWS_REGION": cdk.Stack.of(self).region,
                "AWS_ACCOUNT_ID": cdk.Stack.of(self).account,
                "REPORT_BUCKET": report_bucket.bucket_name,
                "GRADING_BUCKET": gradings_bucket.bucket_name
            },
            logging=ecs.LogDrivers.aws_logs(stream_prefix="WebContainerLogs"))

        service = ecs.FargateService(
            self,
            f"{prefix}-Service",
            cluster=cluster,
            task_definition=fargate_task_definition,
            service_name = f"{prefix}-stl-frontend",
            security_groups=[ecs_security_group],
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PUBLIC),
            assign_public_ip=True
            )
        
        
        bedrock_policy = iam.Policy(
            self,
            f"{prefix}-BedrockPolicy",
            statements=[
                iam.PolicyStatement(
                    actions=["bedrock:InvokeModel","s3:*"],
                    resources=["*"]
                )
            ]
        )
        
        task_role = fargate_task_definition.task_role
        task_role.attach_inline_policy(bedrock_policy)
        
        secret.grant_read(task_role)
        
        https_listener = load_balancer.add_listener(
            f"{prefix}-HTTPSListener",
            port=443,
            protocol=elbv2.ApplicationProtocol.HTTPS,
            certificates=[certificate]
        )
        
        stl_tg = https_listener.add_targets(
            f"{prefix}-TargetGroup",
            target_group_name=f"{prefix}-stl-tg-2",
            port=80,
            protocol=elbv2.ApplicationProtocol.HTTP,
            targets=[service]     
        )
            
        load_balancer.add_listener(
            f"{prefix}-StreamlitListener",
            port=80,
            protocol=elbv2.ApplicationProtocol.HTTP,
            default_action=elbv2.ListenerAction.forward(
                target_groups=[stl_tg]
            )
        )
        
        CfnOutput(
            self,
            f"{prefix}-domainurl",
            value=user_pool_domain.base_url()
        )
    
        