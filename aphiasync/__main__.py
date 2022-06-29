import argparse
import logging
from termcolor import colored
from aphiasync.obisconnector import OBISConnector
from aphiasync import scan


logger = logging.getLogger("aphiasync")
obis_connector = OBISConnector()

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
