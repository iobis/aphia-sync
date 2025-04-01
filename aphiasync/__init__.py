import argparse
import logging
import datetime
from termcolor import colored
import time
from aphiasync.aphiainfo import AphiaInfo
from aphiasync.obisconnector import OBISConnector
import os
from dotenv import load_dotenv
from aphiasync.worms import build_worms_map, CLASSIFICATION_FIELDS, RECORD_FIELDS
import copy
from deepdiff import DeepDiff
import json


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


def sync_dict(dict, obj, fields):
    for field in fields:
        if field in obj:
            dict[field] = obj[field]


def do_fill(worms_map):
    retries = 10

    while True:
        missing = obis_connector.get_missing_ids()
        logger.info(colored(f"Processing {len(missing)} missing IDs", "green"))
        if len(missing) == 0 or retries < 1:
            break
        retries = retries - 1

        for row in missing:
            if row["valid"] is not None and row["valid_found"] is None:
                aphiaid = row["valid"]
                if aphiaid not in worms_map:
                    logger.warning(colored(f"Missing in export: {aphiaid}", "red"))
                    continue
                info = AphiaInfo({}, {}, None, None, None)
                obj = worms_map[aphiaid]
                sync_dict(info.record, obj, RECORD_FIELDS)
                sync_dict(info.classification, obj, CLASSIFICATION_FIELDS)
                obis_connector.update(int(aphiaid), info)
            if row["parent"] is not None and row["parent_found"] is None:
                aphiaid = row["parent"]
                if aphiaid not in worms_map:
                    logger.warning(colored(f"Missing in export: {aphiaid}", "red"))
                    continue
                info = AphiaInfo({}, {}, None, None, None)
                obj = worms_map[aphiaid]
                sync_dict(info.record, obj, RECORD_FIELDS)
                sync_dict(info.classification, obj, CLASSIFICATION_FIELDS)
                obis_connector.update(int(aphiaid), info)


def do_sync(worms_map, aphiaids, skip, dry_run=False):
    for aphiaid, obj in worms_map.items():

        if aphiaids is not None and aphiaid not in aphiaids:
            continue

        if skip is not None and int(aphiaid) < skip:
            continue

        info_db = obis_connector.fetch_aphia_obis(aphiaid)
        if info_db is None:
            logger.warning(colored(f"No record found for {aphiaid}", "red"))
        else:

            # TODO: temporary fix for superdomain
            if info_db.record["AphiaID"] == 1:
                info_db.record["rank"] = "Superdomain"
            if obj["AphiaID"] == 1:
                obj["rank"] = "Superdomain"

            info_db_copy = copy.deepcopy(info_db)
            sync_dict(info_db.record, obj, RECORD_FIELDS)
            sync_dict(info_db.classification, obj, CLASSIFICATION_FIELDS)

            if info_db == info_db_copy:
                logger.info(colored(f"Taxon {aphiaid} has not changed", "grey"))
            else:
                logger.info(colored(f"Updating {aphiaid}", "green"))
                diff = str(DeepDiff(json.loads(str(info_db_copy)), json.loads(str(info_db))))
                logger.info(colored(diff, "blue"))
                if not dry_run:
                    obis_connector.update(aphiaid, info_db)


def bulk_update(sync=True, fill=True, aphiaids=None, skip=None, dry_run=False):
    worms_map = build_worms_map()

    if sync:
        do_sync(worms_map, aphiaids, skip, dry_run)

    if fill:
        do_fill(worms_map)
