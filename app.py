#!/usr/bin/env python3

from aws_cdk import core

from sensitive_data_filter_instream.sensitive_data_filter_instream_stack import SensitiveDataFilterInstreamStack


app = core.App()
SensitiveDataFilterInstreamStack(app, "sensitive-data-filter-instream")

app.synth()
