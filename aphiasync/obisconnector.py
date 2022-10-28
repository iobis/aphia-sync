import logging
import psycopg2
import psycopg2.extras
import datetime
from termcolor import colored
import pyworms
import os
from dotenv import load_dotenv
from aphiasync.aphiainfo import AphiaInfo
import sys


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger(__name__)


class OBISConnector:

    def __init__(self):
        load_dotenv()
        self._logger = logging.getLogger(__name__)
        self.conn = psycopg2.connect(
            "host='%s' dbname='%s' user='%s' password='%s' options='-c statement_timeout=%s'" %
            (os.getenv("DB_HOST"), os.getenv("DB_DB"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_TIMEOUT")))
        self.conn.autocommit = True
        self.cur = self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    def __del__(self):
        self.cur.close()
        self.conn.close()
        self._logger.info(colored("Cleaned up connection", "red"))

    def check(self, aphiaid):
        """Check if the records for an AphiaID have changed and update if necessary."""
        info_db = self.fetch_aphia_obis(aphiaid)
        info_api = self.fetch_aphia_api(aphiaid)

        if info_api is not None and info_db != info_api:

            # WoRMS bug
            if info_api.record is not None and info_api.record["rank"] is None and info_db.record["rank"] is not None:
                logger.warn(colored("No rank for: ", str(info_db), "red"))
                return

            self.update(aphiaid, info_api)
            logger.info(str(aphiaid) + " " + colored(info_api.record["scientificname"], "green"))

            # recursively check parents as well
            if "parentNameUsageID" in info_api.classification and info_api.classification["parentNameUsageID"] is not None:
                parent_id = info_api.classification["parentNameUsageID"]
                assert aphiaid != parent_id
                self.check(parent_id)

        elif info_api.record is not None:
            logger.info(str(aphiaid) + " " + colored(info_api.record["scientificname"], "white"))

        self.set_checked(aphiaid)

    def fetch_aphia_obis(self, aphiaid):
        """Fetch the different records for an AphiaID from the OBIS database."""
        self.cur.execute("""
            select
                record,
                classification,
                distribution,
                bold_id,
                ncbi_id
            from obis.aphia
            where id = %s
        """ % aphiaid)
        res = self.cur.fetchone()
        if res is not None:
            return AphiaInfo(*res)
        else:
            return None

    def fetch_aphia_api(self, aphiaid):
        """Fetch the different records for an AphiaID from the WoRMS API."""
        record = pyworms.aphiaRecordByAphiaID(aphiaid)
        classification = pyworms.aphiaClassificationByAphiaID(aphiaid)
        bold_id = pyworms.aphiaExternalIDByAphiaID(aphiaid, "bold")
        ncbi_id = pyworms.aphiaExternalIDByAphiaID(aphiaid, "ncbi")
        distribution = pyworms.aphiaDistributionsByAphiaID(aphiaid)

        bold_id = int(bold_id[0]) if bold_id is not None and isinstance(bold_id, list) and len(bold_id) > 0 else None
        ncbi_id = int(ncbi_id[0]) if ncbi_id is not None and isinstance(ncbi_id, list) and len(ncbi_id) > 0 else None

        return AphiaInfo(record, classification, distribution, bold_id, ncbi_id)

    def set_checked(self, aphiaid):
        """Update the checked date for an aphia record."""
        self.cur.execute("""
            update obis.aphia
            set last_checked = now(), needs_update = null
            where id = %s
        """ % aphiaid)

    def get_stale_ids(self):
        """Get all AphiaIDs that have not been checked in the last 5 days."""
        self.cur.execute("""
            select id from obis.aphia
            where record::text != 'null' and (last_checked is null or last_checked < now() - interval '5 days') 
            order by needs_update desc nulls last, random()
        """)
        ids = [id[0] for id in self.cur.fetchall()]
        return ids

    def update(self, aphiaid, aphia_info):
        """Update an aphia record in OBIS."""
        self.cur.execute("""
            insert into obis.aphia
                (id, record, classification, distribution, bold_id, ncbi_id, created, updated)
            values
                (%(id)s, %(record)s, %(classification)s, %(distribution)s, %(bold_id)s, %(ncbi_id)s, %(now)s, %(now)s)
            on conflict (id) do update 
            set 
                record = %(record)s,
                classification = %(classification)s,
                distribution = %(distribution)s,
                bold_id = %(bold_id)s,
                ncbi_id = %(ncbi_id)s,
                updated = %(now)s
            where aphia.id = %(id)s
        """, {
            "id": aphiaid,
            "record": psycopg2.extras.Json(aphia_info.record),
            "classification": psycopg2.extras.Json(aphia_info.classification),
            "distribution": psycopg2.extras.Json(aphia_info.distribution),
            "bold_id": aphia_info.bold_id,
            "ncbi_id": aphia_info.ncbi_id,
            "now": datetime.datetime.now()
        })
