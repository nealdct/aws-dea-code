#!/usr/bin/env python3

import aws_cdk as cdk

from codepipeline_glue_deploy.codepipeline_glue_deploy_stack import CodepipelineGlueDeployStack

app = cdk.App()


CodepipelineGlueDeployStack(app, "codepipeline-glue-deploy")

app.synth()
