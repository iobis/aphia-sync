import argparse
import logging
import datetime
from termcolor import colored
import time
from aphiasync.obisconnector import OBISConnector
import os
from dotenv import load_dotenv


def scan():
    while True:
        ids = obis_connector.get_stale_ids()
        logger.info(colored("Processing %s IDs" % (len(ids)), "green"))
        for aphiaid in ids:
            obis_connector.check(aphiaid)
            time.sleep(int(os.getenv("API_INTERVAL")))
        time.sleep(1000)


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)
obis_connector = OBISConnector()
lastchecked = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--ids", nargs="*", help="the identifiers")
args = parser.parse_args()

if args.ids:
    logger.info(colored("Processing %s IDs" % (len(args.ids)), "green"))
    for aphiaid in args.ids:
        obis_connector.check(aphiaid)
else:
    scan()
