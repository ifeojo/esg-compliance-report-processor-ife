from aws_cdk import (
    # Duration,
    Stack,
    aws_lambda as _lambda
)
from constructs import Construct
from .reportupload import ReportUpload
from .cognito import Cognito
from .network import CoreNetwork
import os

architecture = _lambda.Architecture.X86_64
runtime = _lambda.Runtime.PYTHON_3_12

# Docker: Set default platform for commands that take the --platform flag
# This is to ensure all docker build are using the above chip architecture
# regardless the one of the machine running the deployment. Requires buildx.
os.environ["DOCKER_DEFAULT_PLATFORM"] = architecture.docker_platform

class ESGComplianceCdkStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, prefix:str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        #set deploy_mode in cdk.json
        deploy_mode = self.node.try_get_context("deploy_mode") or "full"

        report_stack = ReportUpload(
            self,
            "ReportUpload", 
            prefix = prefix,
            architecture = architecture,
            runtime = runtime
            )
        
        if deploy_mode != "report-only":
        
            cognito_stack = Cognito(
                self,
                "Cognito",
                prefix = prefix,
                report_bucket = report_stack.report_bucket,
                gradings_bucket = report_stack.gradings_bucket
                )
            
            CoreNetwork(
                self,
                "CoreNetwork",
                prefix = prefix,
                user_pool=cognito_stack.pool,
                user_pool_client=cognito_stack.client,
                user_pool_domain=cognito_stack.domain,
                secret = cognito_stack.idpSecrets,
                report_bucket=report_stack.report_bucket,
                gradings_bucket=report_stack.gradings_bucket,
                identity_pool=cognito_stack.identity_pool
                )