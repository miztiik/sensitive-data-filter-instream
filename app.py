#!/usr/bin/env python3

from sensitive_data_filter_instream.stacks.back_end.serverless_kinesis_producer_stack.serverless_kinesis_producer_stack import ServerlessKinesisProducerStack
from sensitive_data_filter_instream.stacks.back_end.firehose_transformation_stack.firehose_tranformation_stack import FirehoseTransformationStack

from aws_cdk import core

app = core.App()

# Kinesis Data Producer on Lambda
serverless_kinesis_producer_stack = ServerlessKinesisProducerStack(
    app,
    f"sensitive-data-producer-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: Kinesis Data Producer on Lambda"
)

# Use Firehose with lambda transformations to filter sensitive data in stream
sensitive_data_filter_stack = FirehoseTransformationStack(
    app,
    f"sensitive-data-filter-stack",
    stack_log_level="INFO",
    src_stream=serverless_kinesis_producer_stack.get_stream,
    description="Miztiik Automation: Firehose with lambda transformations"
)

# Stack Level Tagging
_tags_lst = app.node.try_get_context("tags")

if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            core.Tags.of(app).add(k, v, apply_to_launched_instances=True)


app.synth()
