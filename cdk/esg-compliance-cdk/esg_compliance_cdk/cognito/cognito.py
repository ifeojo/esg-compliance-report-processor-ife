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
    aws_s3 as s3,
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_stepfunctions as states,
    aws_stepfunctions_tasks as tasks,
    aws_iam as iam,
    aws_dynamodb as ddb,
    aws_cognito as cognito,
    aws_secretsmanager as secrets
)
from constructs import Construct



class Cognito(Construct):
    def __init__ (
        self,
        scope: Construct, 
        construct_id:str,
        prefix: str,
        report_bucket: s3.IBucket,
        gradings_bucket: s3.IBucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id)
        
        self.pool = cognito.UserPool(self,
                                f"{prefix}-user-pool",
                                self_sign_up_enabled=True,
                                auto_verify={
                                    'email':True,
                                    'phone': False
                                    },
                                standard_attributes={
                                    'email':{
                                        'required':True,
                                    },
                                    'family_name':{
                                        'required':True,
                                    },
                                    'given_name':{
                                        'required':True,
                                    }
                                }
                                
                                )
        
        pool = self.pool
        
        self.domain = pool.add_domain(f"{prefix}-user-pool-domain",
                                 cognito_domain=cognito.CognitoDomainOptions(domain_prefix=f"esg-demo-{cdk.Stack.of(self).account}")
                                 )
       
        domain = self.domain
        
        self.idpSecrets = secrets.Secret.from_secret_name_v2(
            self,
            'IDPSecrets',
            'idpsecrets'
        )
        
        idpSecrets = self.idpSecrets

        idpName = 'FederateOIDC2'
        
        idp = cognito.UserPoolIdentityProviderOidc(
            self,
            f"{idpName}IdentityProvider",
            client_id=idpSecrets.secret_value_from_json('client_id').unsafe_unwrap(),
            client_secret=idpSecrets.secret_value_from_json('client_secret').unsafe_unwrap(),
            issuer_url=idpSecrets.secret_value_from_json('oidc_issuer').unsafe_unwrap(),
            user_pool=pool,
            attribute_request_method=cognito.OidcAttributeRequestMethod.GET,
            scopes=['openid'],
            name=idpName,
            attribute_mapping=cognito.AttributeMapping(
                given_name= cognito.ProviderAttribute.other('GIVEN_NAME'),
                family_name= cognito.ProviderAttribute.other('FAMILY_NAME'),
                email= cognito.ProviderAttribute.other('EMAIL'),
                preferred_username= cognito.ProviderAttribute.other('UID')  
            )
            
        )
    

        self.client = pool.add_client(f"{prefix}-user-pool-client",
        refresh_token_validity=Duration.days(1),
        access_token_validity=Duration.minutes(30),
        id_token_validity=Duration.minutes(30),
        generate_secret=True,
        o_auth=cognito.OAuthSettings(
            flows=cognito.OAuthFlows(
                authorization_code_grant=True,
                implicit_code_grant=True
            ),
            callback_urls = ["https://esgdemo.ifeojo.people.aws.dev/"],
            logout_urls= ["https://esgdemo.ifeojo.people.aws.dev"],
            scopes=[cognito.OAuthScope.OPENID, cognito.OAuthScope.COGNITO_ADMIN, cognito.OAuthScope.PROFILE]
        ),
        supported_identity_providers=[cognito.UserPoolClientIdentityProvider.custom(idp.provider_name),cognito.UserPoolClientIdentityProvider.COGNITO]
        )
        
        client = self.client
        
        self.identity_pool = cognito.CfnIdentityPool(
            self,
            f"{prefix}-identity-pool",
            identity_pool_name=f"{prefix}-identity-pool",
            allow_unauthenticated_identities=False,
            cognito_identity_providers=[
                cognito.CfnIdentityPool.CognitoIdentityProviderProperty(
                    client_id=client.user_pool_client_id,
                    provider_name=pool.user_pool_provider_name,
                )
            ]
        )
        identity_pool = self.identity_pool
        
        # Create an IAM role for authenticated users
        authenticated_role = iam.Role(self, "AuthenticatedRole",
            assumed_by=iam.FederatedPrincipal(
                "cognito-identity.amazonaws.com",
                conditions={
                    "StringEquals": {
                        "cognito-identity.amazonaws.com:aud": identity_pool.ref
                    },
                    "ForAnyValue:StringLike": {
                        "cognito-identity.amazonaws.com:amr": "authenticated"
                    }
                },
                assume_role_action="sts:AssumeRoleWithWebIdentity"
            ))
        authenticated_role.assume_role_policy.add_statements(
            iam.PolicyStatement(
                actions=["sts:AssumeRoleWithWebIdentity", "sts:TagSession"],
                effect=iam.Effect.ALLOW,
                principals=[
                    iam.FederatedPrincipal(
                        "cognito-identity.amazonaws.com",
                        conditions={
                            "StringEquals": {
                        "cognito-identity.amazonaws.com:aud": identity_pool.ref
                    },
                    "ForAnyValue:StringLike": {
                        "cognito-identity.amazonaws.com:amr": "authenticated"
                    }
                }
            )
        ]
        )
        )
        
        authenticated_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "cognito-identity:GetCredentialsForIdentity"
                ],
                resources=["*"]
            )
        )
        
        authenticated_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:*"
                ],
                resources=[
                    report_bucket.bucket_arn,
                    gradings_bucket.bucket_arn,
                    f"{report_bucket.bucket_arn}/*",
                    f"{gradings_bucket.bucket_arn}/*"
                    ]
            )
        )

        authenticated_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:*"
                ],
                resources=["*"]
            ))
        
        cognito.CfnIdentityPoolRoleAttachment(self, "IdentityPoolRoleAttachment",
            identity_pool_id=identity_pool.ref,
            roles={
                "authenticated": authenticated_role.role_arn
            }
        )
        
    
        
        client.node.add_dependency(idp)            
        # tags
        Tags.of(self).add("project", f"{prefix}-esg-compliance")
        Tags.of(self).add("owner", f"esg-compliance")
      