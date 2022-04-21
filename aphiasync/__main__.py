import argparse
import logging
import datetime
from termcolor import colored
import time
from aphiasync.obisconnector import OBISConnector
import os
from dotenv import load_dotenv


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
        time.sleep(1000)


load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)
obis_connector = OBISConnector()
lastchecked = datetime.datetime.utcnow() - datetime.timedelta(hours=1)

parser = argparse.ArgumentParser()
parser.add_argument("-i", "--ids", nargs="*", help="the identifiers")
parser.add_argument("-n", "--max-names", help="max number of names")
parser.add_argument("-r", "--repeat", action="store_true", help="repeat?")
args = parser.parse_args()

if args.ids:
    logger.info(colored("Processing %s IDs" % (len(args.ids)), "green"))
    for aphiaid in args.ids:
        obis_connector.check(aphiaid)
else:
    max_names = int(args.max_names) if args.max_names else None
    repeat = bool(args.repeat) if args.repeat else False
    scan(repeat, max_names)
