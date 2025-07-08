from dotenv import load_dotenv
from aphiasync.worms import build_worms_map
import sqlite3
import json
from gnparser import parse_to_string
from aphiasync.util import update_hab, update_wrims, update_redlist_by_name, update_external


load_dotenv()


def int_if_not_none(value):
    return int(value) if value is not None and value != "" else None


def make_map_and_import(source_path: str, sqlite_path: str, table_name: str = "parsed", decorate: bool = False):
    """Build a map of WoRMS IDs and records from a specific source, parse the name, and load into sqlite."""

    worms_map = build_worms_map(source_path)
    if decorate:
        update_redlist_by_name(worms_map, "data/redlist.tsv")
        update_hab(worms_map, "/Volumes/acasis/worms/WoRMS_OBIS_HAB")
        update_wrims(worms_map, "/Volumes/acasis/worms/WoRMS_WRiMS")
        update_external(worms_map, "data/external.tsv")
    
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (aphiaid INT PRIMARY KEY, valid_aphiaid INT, canonical TEXT, authorship TEXT, record TEXT, ncbi_id INT, bold_id INT)")

    for index, (aphiaid_str, record) in enumerate(worms_map.items()):

        aphiaid = int(aphiaid_str)
        valid_aphiaid = int(record.get("valid_AphiaID")) if record.get("valid_AphiaID") else None
        original = f"{record.get('scientificname')} {record.get('authority')}" if record.get("authority") else record.get("scientificname")
        parsed_str = parse_to_string(original, "compact", None, 1, 1)
        parsed = json.loads(parsed_str)
        result = (aphiaid, valid_aphiaid, parsed.get("canonical", {}).get("full", None), parsed.get("authorship", {}).get("normalized", None), json.dumps(record), int_if_not_none(record.get("ncbi_id")), int_if_not_none(record.get("bold_id")))
        print(f"\rInserting into {table_name}: {aphiaid} ({index} / {len(worms_map)}) from {source_path}", end="", flush=True)

        c.execute(f"INSERT OR REPLACE INTO {table_name} (aphiaid, valid_aphiaid, canonical, authorship, record, ncbi_id, bold_id) VALUES (?, ?, ?, ?, ?, ?, ?)", result)
        conn.commit()

    c.execute(f"CREATE INDEX IF NOT EXISTS canonical_index ON {table_name} (canonical)")
    if decorate:
        c.execute(f"CREATE INDEX IF NOT EXISTS ncbi_index ON {table_name} (ncbi_id)")
        c.execute(f"CREATE INDEX IF NOT EXISTS bold_index ON {table_name} (bold_id)")
    conn.commit()
    conn.close()
    print(f"\nFinished inserting from {source_path}", flush=True)


# Before running: download OBIS and GBIF WoRMS exports, check other tables in case of decoration
db_path = "/Volumes/acasis/worms/worms_draft_20250707.db"
make_map_and_import("/Volumes/acasis/worms/WoRMS_DwC-A", db_path, decorate=True)  # GBIF export
make_map_and_import("/Volumes/acasis/worms/WoRMS_OBIS", db_path, decorate=True)  # OBIS export
