# -*- coding: utf-8 -*-
"""
.. module: stream_data_producer
    :Actions: Put Records in Kinesis Data Stream 
    :copyright: (c) 2021 Mystique.,
.. moduleauthor:: Mystique
.. contactauthor:: miztiik@github issues
"""


import json
import logging
import datetime
import os
import random
import uuid

import boto3

__author__ = "Mystique"
__email__ = "miztiik@github"
__version__ = "0.0.1"
__status__ = "production"


class GlobalArgs:
    """ Global statics """
    OWNER = "Mystique"
    ENVIRONMENT = "production"
    MODULE_NAME = "stream_data_producer"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    STREAM_NAME = os.getenv("STREAM_NAME", "data_pipe")
    STREAM_AWS_REGION = os.getenv("AWS_REGION", "us-east-1")


def set_logging(lv=GlobalArgs.LOG_LEVEL):
    """ Helper to enable logging """
    logging.basicConfig(level=lv)
    logger = logging.getLogger()
    logger.setLevel(lv)
    return logger


logger = set_logging()


def _gen_uuid():
    """ Generates a uuid string and return it """
    return str(uuid.uuid4())


def send_data(client, data, key, stream_name):
    logger.info(
        f'{{"data":{json.dumps(data)}}}')
    resp = client.put_records(
        Records=[
            {
                "Data": json.dumps(data),
                "PartitionKey": key},
        ],
        StreamName=stream_name
    )
    logger.info(f"Response:{resp}")


def dob_generator(max_age=99, date_fmt="%Y-%m-%d"):
    """ Generates date of births in the specified format and age range"""
    return (
        datetime.datetime.today() - datetime.timedelta(days=random.randint(0, 365 * max_age))
    ).strftime(date_fmt)


client = boto3.client(
    "kinesis", region_name=GlobalArgs.STREAM_AWS_REGION)


def lambda_handler(event, context):
    resp = {"status": False}
    logger.info(f"Event: {json.dumps(event)}")

    _random_user_name = ["Aarakocra", "Aasimar", "Beholder", "Bugbear", "Centaur", "Changeling", "Deep Gnome", "Deva", "Dragonborn", "Drow", "Dwarf", "Eladrin", "Elf", "Firbolg", "Genasi", "Githzerai", "Gnoll", "Gnome", "Goblin", "Goliath", "Hag", "Half-Elf",
                         "Half-Orc", "Halfling", "Hobgoblin", "Kalashtar", "Kenku", "Kobold", "Lizardfolk", "Loxodon", "Mind Flayer", "Minotaur", "Orc", "Shardmind", "Shifter", "Simic Hybrid", "Tabaxi", "Tiefling", "Tortle", "Triton", "Vedalken", "Warforged", "Wilden", "Yuan-Ti"]

    _random_category = ["Books", "Games", "Mobiles", "Groceries", "Shoes", "Stationaries", "Laptops",
                        "Tablets", "Notebooks", "Camera", "Printers", "Monitors", "Speakers", "Projectors", "Cables", "Furniture"]

    _random_category_01 = ["Books", "Electronics"]
    try:
        record_count = 0
        while context.get_remaining_time_in_millis() > 100:
            _s = round(random.random() * 100, 2)
            send_data(
                client,
                {
                    "name": random.choice(_random_user_name),
                    "dob": dob_generator(),
                    "gender": random.choice(["M", "F"]),
                    "ssn_no": f"{random.randrange(100000000,999999999)}",
                    "data_share_consent": bool(random.getrandbits(1)),
                    "evnt_time": datetime.datetime.now().isoformat(),
                },
                _gen_uuid(),
                GlobalArgs.STREAM_NAME
            )
            record_count += 1
            logger.debug(
                f'{{"remaining_time":{context.get_remaining_time_in_millis()}}}')
        resp["record_count"] = record_count
        resp["status"] = True
        logger.info(f"resp: {json.dumps(resp)}")

    except Exception as e:
        logger.error(f"ERROR:{str(e)}")
        resp["error_message"] = str(e)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": resp
        })
    }
