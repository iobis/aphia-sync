import csv
import json
import logging
import os
import re
import sqlite3
from gnparser import parse_to_string


PROGRESS_LOG_INTERVAL = 100000
EXPORT_SQLITE_PROGRESS_INTERVAL = 50_000


RANKS = ["superdomain", "domain", "kingdom", "subkingdom", "infrakingdom", "superphylum", "phylum", "phylum (division)", "subphylum", "subphylum (subdivision)", "infraphylum", "parvphylum", "gigaclass", "megaclass", "superclass", "class", "subclass", "infraclass", "subterclass", "superorder", "order", "suborder", "infraorder", "parvorder", "section", "subsection", "superfamily", "epifamily", "family", "subfamily", "supertribe", "tribe", "subtribe", "genus", "subgenus", "series", "subseries", "species", "subspecies", "natio", "variety", "subvariety", "forma", "subforma", "mutatio"]
RANK_FIELDS = RANKS + [rank + "id" for rank in RANKS]
CLASSIFICATION_FIELDS = RANK_FIELDS + ["parentNameUsage", "parentNameUsageID"]
RECORD_FIELDS = ["taxonRankID", "isBrackish", "valid_authority", "modified", "lsid", "genus", "AphiaID", "citation", "kingdom", "isFreshwater", "isExtinct", "class", "status", "valid_name", "url", "match_type", "isTerrestrial", "family", "rank", "isMarine", "order", "scientificname", "unacceptreason", "phylum", "parentNameUsageID", "authority", "valid_AphiaID"]


def parse_taxon_row(row: dict) -> tuple[str, dict] | None:
    """Parse one taxon.txt row into (taxon_id, obj). Return None if the row is skipped."""
    taxon_rank = row["taxonRank"] if row["taxonRank"] != "" else None
    taxon_id = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["taxonID"]).group(1)

    obj = dict()
    obj["scientificname"] = row["scientificName"]
    obj["citation"] = row["bibliographicCitation"]
    obj["modified"] = row["modified"]
    obj["authority"] = row["scientificNameAuthorship"] if row["scientificNameAuthorship"] != "" else None
    obj["parentNameUsage"] = row["parentNameUsage"]
    obj["status"] = row["taxonomicStatus"]
    # Older DwC-A dumps left taxonRank empty for Biota (AphiaID 1); WoRMS now uses Superdomain.
    if taxon_id == "1" and taxon_rank is None and obj["scientificname"] == "Biota":
        taxon_rank = "Superdomain"
    obj["rank"] = taxon_rank
    obj["url"] = f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={taxon_id}"
    obj["AphiaID"] = int(taxon_id)

    if taxon_rank is not None and "." in taxon_rank:
        return None

    if taxon_rank is not None:
        clean_rank = taxon_rank.lower().replace(" (division)", "").replace(" (subdivision)", "")
        obj[clean_rank] = row["scientificName"]
        obj[clean_rank + "id"] = int(taxon_id)

    accepted_match = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["acceptedNameUsageID"])
    if accepted_match is not None:
        accepted_id = accepted_match.group(1)
        obj["valid_AphiaID"] = int(accepted_id)

    parent_match = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["parentNameUsageID"])
    if parent_match is not None:
        obj["parentNameUsageID"] = int(parent_match.group(1))

    return taxon_id, obj


def read_taxon_txt(export_path: str, worms_map: dict) -> None:
    row_i = 0
    with open(os.path.join(export_path, "taxon.txt")) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            row_i += 1
            if row_i % PROGRESS_LOG_INTERVAL == 0:
                logging.info(
                    "taxon.txt progress: %s rows read (%s)",
                    row_i,
                    export_path,
                )
            parsed = parse_taxon_row(row)
            if parsed is None:
                continue
            taxon_id, obj = parsed
            worms_map[taxon_id] = obj


def rebuild_parents_map(worms_map: dict) -> dict:
    """Build parent_id -> [child taxon_id] lists per child rank from merged worms_map."""
    parents_map = {rank: {} for rank in RANKS}
    for taxon_id, obj in worms_map.items():
        taxon_rank = obj.get("rank")
        if taxon_rank is None or "." in taxon_rank:
            continue
        clean_rank = taxon_rank.lower().replace(" (division)", "").replace(" (subdivision)", "")
        if clean_rank not in parents_map:
            continue
        parent_id = obj.get("parentNameUsageID")
        if parent_id is None:
            continue
        parent_id_str = str(parent_id)
        bucket = parents_map[clean_rank].setdefault(parent_id_str, [])
        bucket.append(taxon_id)
    return parents_map


def propagate_ranks(worms_map: dict, parents_map: dict) -> None:
    for rank in RANKS:
        for parent_id, child_ids in parents_map[rank].items():
            for r in RANKS:
                if parent_id in worms_map and r in worms_map[parent_id]:
                    for child_id in child_ids:
                        worms_map[child_id][r] = worms_map[parent_id][r]
                        worms_map[child_id][r + "id"] = worms_map[parent_id][r + "id"]


def resolve_valid_names(worms_map: dict) -> None:
    for taxon_id, obj in worms_map.items():
        if "valid_AphiaID" not in obj:
            continue
        valid_id = str(obj["valid_AphiaID"])
        if valid_id not in worms_map:
            continue
        obj["valid_name"] = worms_map[valid_id]["scientificname"] if worms_map[valid_id]["scientificname"] != "" else None
        obj["valid_authority"] = worms_map[valid_id]["authority"] if worms_map[valid_id]["authority"] != "" else None


def merge_species_profile(export_path: str, worms_map: dict) -> None:
    with open(os.path.join(export_path, "speciesprofile.txt")) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            taxon_id = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["taxonID"]).group(1)
            if taxon_id not in worms_map:
                continue
            worms_map[taxon_id]["isMarine"] = int(row["isMarine"]) if row["isMarine"] != "" else None
            worms_map[taxon_id]["isBrackish"] = int(row["isBrackish"]) if row["isBrackish"] != "" else None
            worms_map[taxon_id]["isFreshwater"] = int(row["isFreshwater"]) if row["isFreshwater"] != "" else None
            worms_map[taxon_id]["isTerrestrial"] = int(row["isTerrestrial"]) if row["isTerrestrial"] != "" else None
            worms_map[taxon_id]["isExtinct"] = int(row["isExtinct"]) if row["isExtinct"] != "" else None


def build_worms_map(export_paths: list[str]):
    """Load one or more WoRMS export directories.

    All taxon.txt rows are merged first (later exports overwrite earlier on the same Aphia ID),
    then classification ranks are propagated from parents once on the combined map.
    """
    if not export_paths:
        raise ValueError("export_paths must contain at least one path")

    worms_map: dict = {}
    for export_path in export_paths:
        logging.info(f"Reading taxon.txt from {export_path}")
        read_taxon_txt(export_path, worms_map)

    logging.info("Propagating ranks on merged WoRMS map (%s taxa)", len(worms_map))
    parents_map = rebuild_parents_map(worms_map)
    propagate_ranks(worms_map, parents_map)
    resolve_valid_names(worms_map)

    for export_path in export_paths:
        logging.info(f"Merging speciesprofile.txt from {export_path}")
        merge_species_profile(export_path, worms_map)

    return worms_map


def build_worms_map_from_export(export_path: str):
    """Load a single WoRMS export directory (same semantics as build_worms_map with one path)."""
    return build_worms_map([export_path])


def int_if_not_none(value):
    return int(value) if value is not None and value != "" else None


def export_to_sqlite(worms_map: dict, sqlite_path: str, table_name: str = "parsed") -> None:
    """Parse scientific names with gnparser and write the WoRMS map to SQLite.

    Indexes ``canonical``, ``ncbi_id``, and ``bold_id`` for stable query plans regardless
    of how many rows are populated.

    Inserts run in a single transaction with relaxed durability PRAGMAs during the bulk
    phase; per-row commits were the main bottleneck for large maps.
    """
    conn = sqlite3.connect(sqlite_path)
    c = conn.cursor()
    c.execute("PRAGMA journal_mode = MEMORY")
    c.execute("PRAGMA synchronous = OFF")
    c.execute("PRAGMA temp_store = MEMORY")
    c.execute("PRAGMA cache_size = -200000")

    c.execute(
        f"CREATE TABLE IF NOT EXISTS {table_name} (aphiaid INT PRIMARY KEY, valid_aphiaid INT, canonical TEXT, authorship TEXT, record TEXT, ncbi_id INT, bold_id INT)"
    )

    insert_sql = (
        f"INSERT OR REPLACE INTO {table_name} (aphiaid, valid_aphiaid, canonical, authorship, record, ncbi_id, bold_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    n = len(worms_map)
    with conn:
        for index, (aphiaid_str, record) in enumerate(worms_map.items()):
            aphiaid = int(aphiaid_str)
            valid_aphiaid = int(record.get("valid_AphiaID")) if record.get("valid_AphiaID") else None
            original = f"{record.get('scientificname')} {record.get('authority')}" if record.get("authority") else record.get("scientificname")
            parsed_str = parse_to_string(original, "compact", None, 1, 1)
            parsed = json.loads(parsed_str)
            result = (
                aphiaid,
                valid_aphiaid,
                parsed.get("canonical", {}).get("full", None),
                parsed.get("authorship", {}).get("normalized", None),
                json.dumps(record),
                int_if_not_none(record.get("ncbi_id")),
                int_if_not_none(record.get("bold_id")),
            )
            if index % EXPORT_SQLITE_PROGRESS_INTERVAL == 0 or index == n - 1:
                print(f"\rInserting into {table_name}: {index + 1} / {n}", end="", flush=True)

            c.execute(insert_sql, result)

    c.execute("PRAGMA synchronous = NORMAL")
    c.execute(f"CREATE INDEX IF NOT EXISTS canonical_index ON {table_name} (canonical)")
    c.execute(f"CREATE INDEX IF NOT EXISTS ncbi_index ON {table_name} (ncbi_id)")
    c.execute(f"CREATE INDEX IF NOT EXISTS bold_index ON {table_name} (bold_id)")
    conn.commit()
    c.execute("PRAGMA journal_mode = DELETE")
    conn.close()
    print("\nFinished inserting final merged map", flush=True)
