# -*- coding: utf-8 -*-
"""
.. module: stream_data_consumer_using_fireshose_and_store_to_s3
    :Actions: Process kinesis data records
    :copyright: (c) 2021 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""

import json
import base64
import logging
import os

# X-Ray SDK: instrument all SDKs
# from aws_xray_sdk.core import xray_recorder
# from aws_xray_sdk.core import patch_all
# patch_all()

__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "kinesis_firehose_transformer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    ENCODING = "utf-8"


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


def data_scrubber(payload):
    payload["ssn_no"] = f"REDACTED_CONTENT"
    payload["dob"] = f"REDACTED_CONTENT"
    payload["data_redacted"] = True
    return payload


logger = set_logging()


def lambda_handler(event, context):
    resp = {"status": False}
    logger.debug(f"Event: {json.dumps(event)}")
    output = []
    try:
        src_records = []
        resp["total_records"] = len(event["records"])
        # Decode Event for processing
        for record in event["records"]:
            payload = base64.b64decode(
                record["data"]).decode(GlobalArgs.ENCODING)
            event = json.loads(payload)
            # Scrub sensitive content if do not share flag  is set
            if event.get("data_share_consent") == False:
                event = data_scrubber(event)
            src_records.append({
                "recordId": record["recordId"],
                "event": dict(event)  # copy of event
            })

        evnts_processed = 0

        for record in src_records:
            evnt = record["event"]
            # copy existing event
            trans_event = dict(evnt)
            trans_payload = json.dumps(trans_event) + "\n"
            output_record = {
                "recordId": record["recordId"],
                "result": "Ok",
                "data": base64.b64encode(trans_payload.encode(GlobalArgs.ENCODING)).decode(GlobalArgs.ENCODING)
            }
            evnts_processed += 1
            output.append(output_record)

        resp["processed_records"] = evnts_processed
        resp["status"] = True
        logger.info(f"resp: {json.dumps(resp)}")

    except Exception as e:
        logger.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)
        raise

    return {"records": output}
