from dotenv import load_dotenv
from aphiasync.worms import build_worms_map, export_to_sqlite
from aphiasync.util import update_hab, update_wrims, update_redlist_by_name, update_external


load_dotenv()


# Before running: download OBIS and GBIF WoRMS exports, check other tables in case of decoration

db_path = "/Volumes/acasis/worms/worms_draft_20250911.db"
sources = [
    "/Volumes/acasis/worms/WoRMS_DwC-A",  # GBIF export
    "/Volumes/acasis/worms/WoRMS_OBIS",  # OBIS export
]
worms_map = build_worms_map(sources)

update_redlist_by_name(worms_map, "data/redlist.tsv")
update_hab(worms_map, "/Volumes/acasis/worms/WoRMS_OBIS_HAB")
update_wrims(worms_map, "/Volumes/acasis/worms/WoRMS_WRiMS")
update_external(worms_map, "data/external.tsv")

export_to_sqlite(worms_map, db_path)
