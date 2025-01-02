import os
import pathlib
import json
import yaml

import aws_cdk as cdk
from aws_cdk import (
    Tags,
    Stack,
    Duration,
    RemovalPolicy,
    aws_apigateway as apigw,
    aws_s3 as s3,
    aws_s3_notifications as s3n,
    aws_sns as sns,
    aws_sns_subscriptions as subscriptions,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as states,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    aws_dynamodb as ddb,
)
from constructs import Construct


class ReportUpload(Construct):
    def __init__ (
        self,
        scope: Construct, 
        construct_id:str,
        prefix: str,
        architecture: _lambda.Architecture,
        runtime: _lambda.Runtime,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id)

        # Create S3 bucket to store reports
        self.report_bucket = s3.Bucket(
            self,
            f"{prefix}-report-upload-bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            event_bridge_enabled=True
        )
        
        #Create S3 bucket to store gradings
        self.gradings_bucket = s3.Bucket(
            self,
            f"{prefix}-gradings-bucket",
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            versioned=True,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            event_bridge_enabled=True
        )
        
        report_bucket = self.report_bucket
        gradings_bucket = self.gradings_bucket
        
        
        #Notification topic to notify teams when review is over
        sns_topic = sns.Topic(
            self,
            f"{prefix}-report-upload-topic"
        )
        
        #add email
        sns_topic.add_subscription(
            subscriptions.EmailSubscription("XXX@YYY.com")
        )
        
        
        #Table to store Supplier Details and Audit Issues
        supplier_table = ddb.Table(
            self,
            f"{prefix}-supplier-table",
            partition_key=ddb.Attribute(
                name='Company Name',
                type=ddb.AttributeType.STRING
            ),
            sort_key=ddb.Attribute(
                name='AuditDateIssueNumber',
                type=ddb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY
            
        )
        
        #Table to store compliance gradings
        compliance_grading_table = ddb.Table(
            self,
            f"{prefix}-grading-table",
            partition_key=ddb.Attribute(
                name='No',
                type=ddb.AttributeType.STRING
            ),
            sort_key=ddb.Attribute(
                name='Category',
                type=ddb.AttributeType.STRING
            ),
            removal_policy=RemovalPolicy.DESTROY
            
        )
    
        #Loop to form lambda function constructs and layers
        layer_keys = [d for d in os.listdir("lambda_layers") if os.path.isdir(os.path.join("lambda_layers", d))]
        layers = dict(zip(
            layer_keys,
            [
                _lambda.LayerVersion(
                    self,
                    f"{prefix}-{layer_key}-layer",
                    code=_lambda.Code.from_asset(
                        os.path.join("lambda_layers", layer_key),
                        exclude=["requirements.txt"]
                    ),
                    compatible_runtimes=[runtime],
                    compatible_architectures=[architecture],
                    description=f"Third party libraries for {prefix}-{layer_key}",
                ) for layer_key in layer_keys
            ]
        ))
        
        lambda_keys = [d for d in os.listdir("lambdas") if os.path.isdir(os.path.join("lambdas", d))]
        lambda_configs = {}
        for key in lambda_keys:
            with open(os.path.join("lambdas", key, "config.yaml"), "r") as f:
                config = yaml.safe_load(f)
            lambda_configs[key] = config
            
        lambdas = dict(zip(
            lambda_keys,
            [
                _lambda.Function(
                    self,
                    f"{prefix}-{lambda_key}-lambda",
                    function_name=f"{prefix}-{lambda_key}-lambda",
                    code=_lambda.Code.from_asset(
                        os.path.join("lambdas", lambda_key),
                        exclude=["config.yaml"]
                    ),
                    handler="lambda_function.handler",
                    runtime=runtime,
                    architecture=architecture,
                    layers=[layers[layer] for layer in config["layers"]] if config["layers"] else None,
                    timeout=Duration.seconds(config["timeout"]),
                    memory_size=config["memory"],
                ) for lambda_key, config in lambda_configs.items()
            ]
        ))
        
        #add policies to lambda functions
        lambdas["report_split"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=["*"]
            )
        )
        
        lambdas["bedrock_supplier_extraction"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["textract:*", "s3:*", "bedrock:*", "dynamodb:*"],
                resources=["*"]
            )
        )
        
        
        lambdas["extract_nc"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["textract:*", "s3:*","bedrock:*"],
                resources=["*"]
            )
        )
        lambdas["extract_nc"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[supplier_table.table_arn,compliance_grading_table.table_arn]
            )
        )
        
        lambdas["validate_unrated_issues"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[supplier_table.table_arn,compliance_grading_table.table_arn]
            )
        )
        
        lambdas["validate_unrated_issues"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:*"],
                resources=["*"]
            )
        )
        
        lambdas["get_nc"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[supplier_table.table_arn]
            )
        )
        
        lambdas["get_nc"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=["*"]
            )
        )
        
        lambdas["get_status"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=["*"]
            )
        )
        
        lambdas["generate_email"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=["*"]
            )
        )
        
        lambdas["send_emails"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:*"],
                resources=["*"]
            )
        )
        
        lambdas["send_emails"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[supplier_table.table_arn]
            )
        )
        
        lambdas["generate_email"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[supplier_table.table_arn]
            )
        )
        
        
        lambdas["send_emails"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["sns:publish"],
                resources=[sns_topic.topic_arn]
            )
        )
        lambdas["generate_email"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["bedrock:*"],
                resources=["*"]
            )
        )
        
        lambdas["upload_grading"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["dynamodb:*"],
                resources=[compliance_grading_table.table_arn]
            )
        )
        
        lambdas["upload_grading"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["s3:*"],
                resources=[f"{gradings_bucket.bucket_arn}/*",gradings_bucket.bucket_arn]
            )
        )
        
        gradings_bucket.add_event_notification(s3.EventType.OBJECT_CREATED, s3n.LambdaDestination(lambdas["upload_grading"]), s3.NotificationKeyFilter(prefix='gradings/'))
        
        
        #add environment variables to lambda functions
        lambdas["bedrock_supplier_extraction"].add_environment('SUPPLIER_TABLE', supplier_table.table_name)
        lambdas["extract_nc"].add_environment('SUPPLIER_TABLE', supplier_table.table_name)
        lambdas["extract_nc"].add_environment('GRADINGS_TABLE', compliance_grading_table.table_name)
        lambdas["get_nc"].add_environment('SUPPLIER_TABLE', supplier_table.table_name)
        lambdas["validate_unrated_issues"].add_environment('GRADINGS_TABLE', compliance_grading_table.table_name)
        lambdas["validate_unrated_issues"].add_environment('SUPPLIER_TABLE', supplier_table.table_name)
        lambdas["send_emails"].add_environment('TOPIC_ARN', sns_topic.topic_arn)
        lambdas["send_emails"].add_environment('SUPPLIER_TABLE', supplier_table.table_name)
        lambdas["generate_email"].add_environment('SUPPLIER_TABLE', supplier_table.table_name)
        lambdas["upload_grading"].add_environment('GRADINGS_TABLE', compliance_grading_table.table_name)
        
        #Create state machine
        report_split_job = tasks.LambdaInvoke(
            self,
            f"{prefix}-report-splitting-task",
            lambda_function=lambdas["report_split"],
            payload=states.TaskInput.from_object({
                "detail": states.JsonPath.entire_payload
            }),
            # select the part of the task result that you want to include in the output
            result_selector={
                "report_split_output":states.JsonPath.string_at("$.Payload")
            }
        )
        
        bedrock_supplier_details_job = tasks.LambdaInvoke(
            self,
            f"{prefix}-supplier-details-task",
            lambda_function=lambdas["bedrock_supplier_extraction"],
            payload=states.TaskInput.from_json_path_at("$.report_split_output.shortened_URIs"),
            # select the part of the task result that you want to include in the output
            result_selector={
                "task_result":states.JsonPath.string_at("$.Payload")
            },
            # to include the original input in the output
            # state where you want to store the output of this task
            result_path="$.supplier_details_output"
        )

        
        nc_map = states.Map(self, "Map State",
                            max_concurrency=10,
                            items_path=states.JsonPath.string_at("$.supplier_details_output.task_result.nc_uri_list"),
                            item_selector={
                                "supplier_uri": states.JsonPath.string_at("$.supplier_details_output.task_result.supplier_uri"),
                                "nc_uri": states.JsonPath.string_at("$$.Map.Item.Value.nc_uri"),
                                "clause": states.JsonPath.string_at("$$.Map.Item.Value.clause"),
                                "section": states.JsonPath.string_at("$$.Map.Item.Value.section"),
                                "company_name": states.JsonPath.string_at("$.supplier_details_output.task_result.company_name"),
                                "audit_date": states.JsonPath.string_at("$.supplier_details_output.task_result.audit_date")
                                },
                                result_path="$.nc_map_output"
                                )
        
        extract_nc_job = tasks.LambdaInvoke(
            self,
            f"{prefix}-extract-nc-task",
            lambda_function=lambdas["extract_nc"],
            payload=states.TaskInput.from_json_path_at("$"),
            result_selector={
                "task_result":states.JsonPath.string_at("$.Payload")
            }
            ,
            result_path="$.extract_nc"
        )
        
        validation_choice = states.Choice(
            self,
            f"{prefix}-validation-choice"
        )
        
        validation_condition_1 = states.Condition.number_equals("$.extract_nc.task_result.count_issue",0)
        validation_condition_2 = states.Condition.number_equals("$.extract_nc.task_result.count_bedrock",0)

        
        # send_approval_emails_job = tasks.LambdaInvoke(
        #     self,
        #     f"{prefix}-send-emails-request-task",
        #     state_name="SendApprovalRequest",
        #     lambda_function=lambdas["send_emails"],
        #     integration_pattern=states.IntegrationPattern.WAIT_FOR_TASK_TOKEN,
        #     payload=states.TaskInput.from_object(
        #         {
        #             "token": states.JsonPath.task_token,
        #             "supplier_details": states.JsonPath.string_at("$.supplier_uri"),
        #             "state_name": states.JsonPath.state_name,
        #             "company_name":states.JsonPath.string_at("$.company_name"),
        #             "audit_date":states.JsonPath.string_at("$.audit_date")
        #         }
        #     ),
        #     result_selector={
        #         "task_result":states.JsonPath.string_at("$.status")
        #     },
        #     result_path="$.approval_status"
        # )
        

        
        # send_confirmation_emails_job = tasks.LambdaInvoke(
        #     self,
        #     f"{prefix}-send-emails-confirmtions-task",
        #     state_name="SendConfirmation",
        #     lambda_function=lambdas["send_emails"],
        #     payload=states.TaskInput.from_object(
        #         {
        #             "supplier_details": states.JsonPath.string_at("$.supplier_uri"),
        #             "state_name": states.JsonPath.state_name,
        #             "status": states.JsonPath.string_at("$.approval_status.task_result")
        #         }
        #     ),
        #     result_selector={
        #         "task_result":states.JsonPath.string_at("$.Payload")
        #     },
        #     result_path="$.confirmation_email_output"
        # )
        
        error_pass = states.Pass(self,"ErrorPass")
        map_pass = states.Pass(self,"MapSuccess")
        success_pass= states.Pass(self,"SuccessPass")
      
        extract_nc_job.add_catch(
            errors=["States.ALL"],
            result_path="$.error",
            handler=error_pass
        )
        
        extract_nc_job.add_retry(
            max_attempts=6,
            interval=Duration.seconds(60),
            backoff_rate=2,
            errors=[
            "Lambda.ProvisionedThroughputExceededException"]
        )
        validate_unrated_issues_job = tasks.LambdaInvoke(
            self,
            f"{prefix}-validate-unrated-issues-task",
            lambda_function=lambdas["validate_unrated_issues"],
            payload=states.TaskInput.from_json_path_at("$.extract_nc.task_result"),
            result_selector={
                "task_result":states.JsonPath.string_at("$.Payload")
            },
            result_path="$.validate_unrated_issues"
        )
        
        get_nc_job = tasks.LambdaInvoke(
        self,
        f"{prefix}-get-nc-task",
        lambda_function=lambdas["get_nc"],
        payload=states.TaskInput.from_json_path_at("$.extract_nc.task_result"),
        result_selector={
            "task_result":states.JsonPath.string_at("$.Payload")
        },
        result_path="$.get_nc"
    )
        
        generate_email_job = tasks.LambdaInvoke(
        self,
        f"{prefix}-generate-email-task",
        lambda_function=lambdas["generate_email"],
        payload=states.TaskInput.from_json_path_at("$.nc_map_output[0].extract_nc.task_result"),
        result_selector={
            "task_result":states.JsonPath.string_at("$.Payload")
        },
        result_path="$.generate_email"
    )
        
        get_status_job = tasks.LambdaInvoke(
        self,
        f"{prefix}-get-status-task",
        lambda_function=lambdas["get_status"],
        payload = states.TaskInput.from_json_path_at("$.generate_email.task_result"),
        # payload=states.TaskInput.from_json_path_at("$.nc_map_output[0].extract_nc.task_result"),
        result_selector={
            "task_result":states.JsonPath.string_at("$.Payload")
        },
        result_path="$.get_status"
    )
        
        nc_map_definition = (
            extract_nc_job
            .next(validation_choice
                .when(validation_condition_1, get_nc_job)
                .when(validation_condition_2, get_nc_job)
                .otherwise(validate_unrated_issues_job
                           .next(get_nc_job))
                .afterwards()
                           )
                
            )
    
        nc_map.item_processor(nc_map_definition)
        
        state_definition = (
            report_split_job
            .next(bedrock_supplier_details_job)
            .next(nc_map)
            .next(generate_email_job)
            .next(get_status_job)
            .next(success_pass)
        )
        
        # state_definition = (
        #     report_split_job
        #     .next(bedrock_supplier_details_job)
        #     .next(extract_nc_job)
        #     .next(validation_choice
        #         .when(validation_condition_1,send_approval_emails_job)
        #         .when(validation_condition_2,send_approval_emails_job)
        #         .otherwise(validate_unrated_issues_job
        #                    .next(send_approval_emails_job)
        #                    )
        #         .afterwards()
        #         )
        #     # .next(send_confirmation_emails_job)
        #     .next(success_pass))

        state_machine = states.StateMachine(
            self,
            f"{prefix}-report-upload-state-machine",
            state_machine_name=f"{prefix}-report-upload-state-machine",
            definition_body=states.DefinitionBody.from_chainable(state_definition),
            timeout=Duration.minutes(5),
        )
        
        #Create Amazon EventBridge Rule that executes a state machine following a PutObject
        
        put_report_rule = events.Rule(
            self,
            f"{prefix}-put-report-rule",
            event_pattern=events.EventPattern(
                source=["aws.s3"],
                detail_type=["Object Created"],
                detail={
                    "bucket": {
                        "name": [report_bucket.bucket_name]
                    },
                    "object": {
                        "key": [{ "wildcard": "*/inputs/*" }]
                    }
                }
            )
        )
        
        put_report_rule.add_target(targets.SfnStateMachine(state_machine))
        
        lambdas["email_approved"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:sendTaskSuccess"],
                resources=["*"]
            )
        )
        
        lambdas["email_rejected"].add_to_role_policy(
            iam.PolicyStatement(
                actions=["states:sendTaskFailure"],
                resources=["*"]
            )
        )
        
        api = apigw.RestApi(
            self,
            f"{prefix}-report-upload-api",
            description=f"API for {prefix} approval requests",
        )
        
        approve_integration = apigw.LambdaIntegration(
            lambdas["email_approved"],
        )
        reject_integration = apigw.LambdaIntegration(
            lambdas["email_rejected"],
        )
        
        api.root.add_resource("approve").add_method("GET", approve_integration)
        api.root.add_resource("reject").add_method("GET", reject_integration)
        
        lambdas["send_emails"].add_environment('BASE_URL', api.url)
        
        # tags
        Tags.of(self).add("project", f"{prefix}-esg-compliance")
        Tags.of(self).add("owner", f"esg-compliance")
      