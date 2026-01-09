import json
from os import listdir
from constructs import Construct

import aws_cdk.aws_codecommit as code_commit
import aws_cdk.aws_codepipeline as code_pipeline
import aws_cdk.aws_events as events
import aws_cdk.aws_iam as iam
import aws_cdk.aws_kms as kms
import aws_cdk.aws_lambda as lambda_
import aws_cdk.aws_s3 as s3
import aws_cdk as cdk
import os


class CodepipelineGlueDeployStack(cdk.Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        etl_repository = code_commit.Repository(
            self,
            id='etl_repository',
            repository_name=construct_id,
            code=code_commit.Code.from_directory("etl_app", "master")
        )

        cdk.CfnOutput(
            self,
            id='etl_repository_url_http',
            value=etl_repository.repository_clone_url_http,
        )

        cdk.CfnOutput(
            self,
            id='etl_repository_url_ssh',
            value=etl_repository.repository_clone_url_ssh
        )

        pipeline_artifact_store_bucket = s3.Bucket(
            self,
            id='pipeline_artifact_store_bucket',
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        pipeline_role = iam.Role(
            self,
            id='pipeline_role',
            assumed_by=iam.ServicePrincipal('codepipeline.amazonaws.com'),
            inline_policies={
                'CodeCommit': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                'codecommit:GetBranch',
                                'codecommit:GetCommit',
                                'codecommit:UploadArchive',
                                'codecommit:GetUploadArchiveStatus'
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=[
                                etl_repository.repository_arn
                            ]
                        )
                    ]
                )
            }
        )

        pipeline_artifact_store_encryption_key = kms.Key(
            self,
            id='pipeline_artifact_store_encryption_key',
            removal_policy=cdk.RemovalPolicy.DESTROY
        )

        cloud_watch_policy = iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    actions=[
                        'cloudwatch:*',
                        's3:*',
                    ],
                    effect=iam.Effect.ALLOW,
                    resources=['*']
                ),
                iam.PolicyStatement(
                    actions=['logs:*'],
                    effect=iam.Effect.ALLOW,
                    resources=['*']
                )
            ]
        )

        glue_role = iam.Role(
            self,
            id='glue_role',
            assumed_by=iam.ServicePrincipal('glue.amazonaws.com'),
            inline_policies={
                'CloudWatch': cloud_watch_policy
            }
        )

        glue_role.add_managed_policy(iam.ManagedPolicy.from_aws_managed_policy_name('service-role/AWSGlueServiceRole'))

        lambda_role = iam.Role(
            self,
            id='lambda_role',
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                'Launch': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                'glue:CreateJob',
                                'glue:StartJobRun'
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=[
                                '*'
                            ]
                        ),
                        iam.PolicyStatement(
                            actions=['iam:PassRole'],
                            effect=iam.Effect.ALLOW,
                            resources=[
                                glue_role.role_arn
                            ]
                        )
                    ]
                ),
                'CloudWatch': cloud_watch_policy
            }
        )

        pipeline_artifact_store_encryption_key.add_to_resource_policy(
            statement=iam.PolicyStatement(
                actions=[
                    "kms:Decrypt",
                    "kms:DescribeKey",
                    "kms:Encrypt",
                    "kms:ReEncrypt*",
                    "kms:GenerateDataKey*"
                ],
                effect=iam.Effect.ALLOW,
                resources=['*'],
                principals=[
                    iam.ArnPrincipal(
                        arn=pipeline_role.role_arn
                    ),
                    iam.ArnPrincipal(
                        arn=lambda_role.role_arn
                    ),
                    iam.ArnPrincipal(
                        arn=glue_role.role_arn
                    )
                ]
            )
        )

        pipeline_artifact_store_encryption_key_props = code_pipeline.CfnPipeline.EncryptionKeyProperty(
            id=pipeline_artifact_store_encryption_key.key_id,
            type='KMS'
        )

        pipeline_artifact_store = code_pipeline.CfnPipeline.ArtifactStoreProperty(
            location=pipeline_artifact_store_bucket.bucket_name,
            type='S3',
            encryption_key=pipeline_artifact_store_encryption_key_props
        )

        s3_statement = iam.PolicyStatement(
            actions=[
                "s3:Get*",
                "s3:List*",
                "s3:DeleteObject*",
                "s3:PutObject*",
                "s3:Abort*"
            ],
            effect=iam.Effect.ALLOW,
            resources=[
                f'arn:aws:s3:::{pipeline_artifact_store.location}',
                f'arn:aws:s3:::{pipeline_artifact_store.location}/*'
            ]
        )

        pipeline_role.add_to_policy(
            statement=s3_statement
        )

        lambda_role.add_to_policy(
            statement=s3_statement
        )

        glue_role.add_to_policy(
            statement=s3_statement
        )

        _lambda = lambda_.Function(
            self,
            id='_lambda',
            code=lambda_.Code.from_asset('lambda_etl_launch'),
            handler='lambda_etl_launch.lambda_handler',
            runtime=lambda_.Runtime.PYTHON_3_8,
            role=lambda_role,
            retry_attempts=0,
            environment={
                'REPOSITORY_NAME': etl_repository.repository_name,
                'FILENAME': 'etl.py'
            }
        )

        pipeline_role.add_to_policy(
            statement=iam.PolicyStatement(
                actions=[
                    'lambda:InvokeFunction'
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    _lambda.function_arn
                ]
            )
        )

        pipeline = code_pipeline.CfnPipeline(
            self,
            id='pipeline',
            name=construct_id,
            artifact_store=pipeline_artifact_store,
            role_arn=pipeline_role.role_arn,
            stages=[
                code_pipeline.CfnPipeline.StageDeclarationProperty(
                    name='Source',
                    actions=[
                        code_pipeline.CfnPipeline.ActionDeclarationProperty(
                            name='Source',
                            action_type_id=code_pipeline.CfnPipeline.ActionTypeIdProperty(
                                category='Source',
                                owner='AWS',
                                provider='CodeCommit',
                                version='1'
                            ),
                            configuration={
                                'RepositoryName': etl_repository.repository_name,
                                'BranchName': 'master',
                                'PollForSourceChanges': False,
                            },
                            output_artifacts=[
                                code_pipeline.CfnPipeline.OutputArtifactProperty(name='SourceCode')
                            ]
                        ),
                    ]
                ),
                code_pipeline.CfnPipeline.StageDeclarationProperty(
                    name='Deploy',
                    actions=[
                        code_pipeline.CfnPipeline.ActionDeclarationProperty(
                            name='Deploy',
                            action_type_id=code_pipeline.CfnPipeline.ActionTypeIdProperty(
                                category='Invoke',
                                owner='AWS',
                                provider='Lambda',
                                version='1'
                            ),
                            configuration={
                                'FunctionName': _lambda.function_name,
                                'UserParameters': json.dumps({
                                    'glue_job_name': construct_id,
                                    'glue_role': glue_role.role_name,
                                })
                            },
                            input_artifacts=[
                                code_pipeline.CfnPipeline.InputArtifactProperty(
                                    name='SourceCode'
                                )
                            ]
                        )
                    ]
                )
            ]
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'codepipeline:PutJobSuccessResult',
                    'codepipeline:PutJobFailureResult',
                ],
                effect=iam.Effect.ALLOW,
                resources=['*']
            )
        )

        lambda_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    'codecommit:GetFile'
                ],
                effect=iam.Effect.ALLOW,
                resources=[
                    etl_repository.repository_arn
                ]
            )
        )

        event_rule_role = iam.Role(
            self,
            id='event_rule_role',
            role_name='EventRuleTriggerPipeline' + pipeline.name,
            assumed_by=iam.ServicePrincipal(
                service='events.amazonaws.com'
            ),
            inline_policies={
                'CloudWatchEventPipelineExecution': iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                'codepipeline:StartPipelineExecution'
                            ],
                            effect=iam.Effect.ALLOW,
                            resources=[f'arn:aws:codepipeline:{self.region}:{self.account}:{pipeline.name}']
                        )
                    ],
                )
            },
            path='/'
        )

        _ = events.CfnRule(
            self,
            id='event_rule',
            event_pattern={
                'source': ['aws.codecommit'],
                'detail-type': ['CodeCommit Repository State Change'],
                'resources': [etl_repository.repository_arn],
                'detail': {
                    'event': [
                        'referenceCreated',
                        'referenceUpdated'
                    ],
                    'referenceType': ['branch'],
                    'referenceName': ['master']
                }
            },
            targets=[
                events.CfnRule.TargetProperty(
                    arn=f'arn:aws:codepipeline:{self.region}:{self.account}:{pipeline.name}',
                    id=f'codepipeline-{pipeline.name}',
                    role_arn=event_rule_role.role_arn,
                )
            ]
        )
