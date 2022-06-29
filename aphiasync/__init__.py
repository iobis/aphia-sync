import argparse
import logging
import datetime
from termcolor import colored
import time
from aphiasync.obisconnector import OBISConnector
import os
from dotenv import load_dotenv


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger("aphiasync")
obis_connector = OBISConnector()


def scan(repeat=False, max_names=None):
    while True:
        ids = obis_connector.get_stale_ids()
        if max_names is not None:
            ids = ids[:max_names]
        logger.info(colored("Processing %s IDs" % (len(ids)), "green"))
        for aphiaid in ids:
            obis_connector.check(aphiaid)
            time.sleep(int(os.getenv("API_INTERVAL")))
        if repeat is False:
            break
