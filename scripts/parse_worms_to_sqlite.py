from dotenv import load_dotenv
from aphiasync.worms import build_worms_map
import sqlite3
import json
from gnparser import parse_to_string
from aphiasync.util import update_hab, update_wrims, update_redlist, update_external


load_dotenv()


def int_if_not_none(value):
    return int(value) if value is not None and value != "" else None


def make_map_and_import(export_path: str):

    worms_map = build_worms_map(export_path)
    update_hab(worms_map, "/Users/pieter/Desktop/temp/WoRMS_OBIS_HAB")
    update_wrims(worms_map, "/Users/pieter/Desktop/temp/WoRMS_WRiMS")
    update_redlist(worms_map, "/Users/pieter/Desktop/temp/redlist.tsv")
    update_external(worms_map, "/Users/pieter/Desktop/temp/external.tsv")
    
    conn = sqlite3.connect("data/worms_parsed.db")
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS parsed (aphiaid INT PRIMARY KEY, valid_aphiaid INT, canonical TEXT, authorship TEXT, record TEXT, ncbi_id INT, bold_id INT)")

    for index, (aphiaid_str, record) in enumerate(worms_map.items()):

        aphiaid = int(aphiaid_str)
        valid_aphiaid = int(record.get("valid_AphiaID")) if record.get("valid_AphiaID") else None
        original = f"{record.get('scientificname')} {record.get('authority')}" if record.get("authority") else record.get("scientificname")
        parsed_str = parse_to_string(original, "compact", None, 1, 1)
        parsed = json.loads(parsed_str)
        result = (aphiaid, valid_aphiaid, parsed.get("canonical", {}).get("full", None), parsed.get("authorship", {}).get("normalized", None), json.dumps(record), int_if_not_none(record.get("ncbi_id")), int_if_not_none(record.get("bold_id")))
        print(f"\rInserting into parsed: {aphiaid} ({index} / {len(worms_map)}) from {export_path}", end="", flush=True)

        c.execute("INSERT OR REPLACE INTO parsed (aphiaid, valid_aphiaid, canonical, authorship, record, ncbi_id, bold_id) VALUES (?, ?, ?, ?, ?, ?, ?)", result)
        conn.commit()

    c.execute("CREATE INDEX IF NOT EXISTS canonical_index ON parsed (canonical)")
    c.execute("CREATE INDEX IF NOT EXISTS ncbi_index ON parsed (ncbi_id)")
    c.execute("CREATE INDEX IF NOT EXISTS bold_index ON parsed (bold_id)")
    conn.commit()
    conn.close()


make_map_and_import("/Users/pieter/Desktop/temp/WoRMS_DwC-A")
make_map_and_import("/Users/pieter/Desktop/temp/WoRMS_OBIS")
