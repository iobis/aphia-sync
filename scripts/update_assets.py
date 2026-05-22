from dotenv import load_dotenv
from aphiasync.aphiainfo import AphiaInfo
from aphiasync.sync import sync_dict
from aphiasync.worms import build_worms_map, export_to_sqlite, RANKS, CLASSIFICATION_FIELDS, RECORD_FIELDS
from aphiasync.util import update_hab, update_wrims, update_redlist_by_name, update_external
import copy
import json
import logging
import os
import psycopg2
import psycopg2.extras
from termcolor import colored


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


ADDED_SAMPLE_COUNT = 10
SKIPPED_REPLACEMENT_SAMPLE_COUNT = 8
LINEAGE_MISMATCH_SAMPLE_COUNT = 10
load_dotenv()


def clean_rank(taxon_rank):
    if taxon_rank is None or (isinstance(taxon_rank, str) and "." in taxon_rank):
        return None
    return taxon_rank.lower().replace(" (division)", "").replace(" (subdivision)", "")


def is_species_rank(record):
    return clean_rank(record.get("rank")) == "species"


def is_accepted_record(record):
    s = record.get("status")
    if s is None:
        return False
    return str(s).strip().lower() == "accepted"


def parent_rank_index(old_parent):
    pr = clean_rank(old_parent.get("rank"))
    if pr is None or pr not in RANKS:
        return None
    return RANKS.index(pr)


def lineage_diffs_vs_parent(new_taxon, old_parent, parent_rank_index):
    """
    Each differing rank: (rank, new_name, new_id, old_name, old_id).
    Empty if new_taxon matches old_parent from superdomain through parent's rank (inclusive).
    """
    diffs = []
    for idx, r in enumerate(RANKS):
        if idx > parent_rank_index:
            break
        nv = (new_taxon.get(r), new_taxon.get(r + "id"))
        ov = (old_parent.get(r), old_parent.get(r + "id"))
        if nv != ov:
            diffs.append((r, nv[0], nv[1], ov[0], ov[1]))
    return diffs


def format_lineage_diff_line(d):
    r, nn, nid, on, oid = d
    new_side = colored(f"{nn!r}  [id={nid!r}]", "green")
    old_side = colored(f"{on!r}  [id={oid!r}]", "red")
    return f"      {colored(r + ':', 'white', attrs=['bold'])}  NEW {new_side}  │  OLD {old_side}"


def forbidden_replacements(old_worms_map, new_worms_map):
    """
    Returns:
      forbidden: new Aphia ID strings that are the accepted replacement for an old accepted
        species still on the backbone (adding them would parallel deprecated old nodes).
      reasons: forbidden_id -> (old_uid, old_scientificname, valid_AphiaID) for logging.
    """
    forbidden = set()
    reasons = {}
    for uid, u_old in old_worms_map.items():
        if not is_species_rank(u_old) or not is_accepted_record(u_old):
            continue
        u_new = new_worms_map.get(uid)
        if not u_new:
            continue
        vu = u_new.get("valid_AphiaID")
        if vu is None:
            continue
        if str(vu) != uid:
            tid = str(vu)
            forbidden.add(tid)
            reasons[tid] = (uid, u_old.get("scientificname"), vu)
    return forbidden, reasons


def print_merge_report(
    n_missing_any_rank,
    n_species_candidates,
    added,
    n_no_parent,
    n_parent_missing,
    n_bad_parent_rank,
    n_lineage_mismatch,
    n_forbidden_replacement,
    n_unaccepted_no_valid_in_old,
    n_unaccepted_valid_not_accepted_in_old,
    added_examples,
    forbidden_examples,
    lineage_mismatch_examples,
):
    w, g, y, r_, c, m, b = "white", "green", "yellow", "red", "cyan", "magenta", "blue"

    print()
    print(colored("═" * 72, c))
    print(colored("  Merge: new species → old map (backbone-safe)", c, attrs=["bold"]))
    print(colored("═" * 72, c))
    print()
    print(
        f"  {colored('Taxa in new but not in old (any rank)', w)}: "
        f"{colored(str(n_missing_any_rank), y, attrs=['bold'])}"
    )
    print(
        f"  {colored('Of those, rank = Species (candidates)', w)}: "
        f"{colored(str(n_species_candidates), y, attrs=['bold'])}"
    )
    print(f"  {colored('Copied into old map', w)}: {colored(str(added), g, attrs=['bold'])}")
    print()
    print(colored("  Not copied — reasons (species candidates only)", y, attrs=["bold"]))
    rows = [
        ("No parentNameUsageID", n_no_parent, m),
        ("Parent missing in old map", n_parent_missing, r_),
        ("Could not resolve parent rank", n_bad_parent_rank, m),
        ("Lineage ≠ old parent (classification)", n_lineage_mismatch, r_),
        ("New accepted replaces old accepted (forbidden)", n_forbidden_replacement, r_),
        ("Unaccepted: valid_AphiaID not in old map", n_unaccepted_no_valid_in_old, m),
        ("Unaccepted: valid in old but not accepted there", n_unaccepted_valid_not_accepted_in_old, m),
    ]
    for label, n, col in rows:
        print(f"    {label}: {colored(str(n), col, attrs=['bold'])}")
    print()

    if lineage_mismatch_examples:
        print(colored("─" * 72, c))
        print(
            colored(
                "  Lineage mismatch vs old parent — sample (max %s)" % LINEAGE_MISMATCH_SAMPLE_COUNT,
                r_,
                attrs=["bold"],
            )
        )
        print(colored("─" * 72, c))
        for i, (tid, parent_key, sci_name, rank, diffs) in enumerate(lineage_mismatch_examples, 1):
            print()
            title = (
                f"{colored('[' + str(i) + ']', b, attrs=['bold'])}  "
                f"AphiaID {tid}  parent={parent_key}  {colored(sci_name or '?', w)}  ({rank or '?'})"
            )
            print(title)
            for d in diffs:
                print(format_lineage_diff_line(d))
        print()

    if added and added_examples:
        print(colored("─" * 72, c))
        print(
            colored(
                "  Copied — sample species (max %s)" % ADDED_SAMPLE_COUNT,
                g,
                attrs=["bold"],
            )
        )
        print(colored("─" * 72, c))
        for i, sample in enumerate(added_examples, 1):
            tid, pk, name, rank, auth, psci, prank, status, valid_id = sample
            print()
            print(
                f"{colored('[' + str(i) + ']', g, attrs=['bold'])}  "
                f"AphiaID {colored(tid, w)}  {colored(name or '?', w, attrs=['bold'])}"
            )
            print(f"      {colored('status', 'white')}: {status or '?'}")
            if valid_id is not None:
                print(f"      {colored('valid_AphiaID', 'white')}: {valid_id}")
            if auth:
                print(f"      {colored('authority', 'white')}: {auth}")
            print(f"      {colored('parent (old map)', 'white')}: AphiaID {pk}  {colored(psci or '?', y)}  ({prank or '?'})")
        print()

    if forbidden_examples:
        print(colored("─" * 72, c))
        print(
            colored(
                "  Skipped — new accepted would replace old backbone species (sample)",
                r_,
                attrs=["bold"],
            )
        )
        print(colored("─" * 72, c))
        for i, (tid, old_uid, old_name, new_valid) in enumerate(forbidden_examples, 1):
            print(
                f"    {colored('[' + str(i) + ']', r_, attrs=['bold'])}  "
                f"would add AphiaID {tid}  —  old accepted {old_uid} ({old_name!r}) "
                f"now has valid_AphiaID={new_valid} in new export"
            )
        print()

    print(colored("═" * 72, c))
    print()


def merge_new_species_into_old_when_compatible(old_worms_map, new_worms_map):
    """
    Add species from new_worms_map that are absent from old_worms_map only if:

    - Rank is Species.
    - Direct parent exists in old_worms_map.
    - Classification on the new species (all ranks from superdomain through the direct
      parent's rank, name + id) equals the old parent's record for those ranks.
    - If accepted in new: not the accepted replacement for a species still accepted on the
      old backbone (see forbidden_replacements).
    - If not accepted in new: valid_AphiaID must refer to an accepted species in old_worms_map
      (including one added earlier in this same run; candidates are processed accepted-first).

    Returns a dict of aphiaid str -> record to merge into the old map (does not mutate old_worms_map).
    """
    additions = {}
    missing_any = [tid for tid in new_worms_map if tid not in old_worms_map]
    n_missing_any_rank = len(missing_any)

    species_candidates = [tid for tid in missing_any if is_species_rank(new_worms_map[tid])]
    # Accepted first so unaccepted synonyms can resolve valid_AphiaID already copied into old_worms_map.
    species_candidates.sort(
        key=lambda tid: (0 if is_accepted_record(new_worms_map[tid]) else 1, int(tid))
    )
    n_species_candidates = len(species_candidates)

    forbidden, replacement_reasons = forbidden_replacements(old_worms_map, new_worms_map)

    added = 0
    n_no_parent = 0
    n_parent_missing = 0
    n_bad_parent_rank = 0
    n_lineage_mismatch = 0
    n_forbidden_replacement = 0
    n_unaccepted_no_valid_in_old = 0
    n_unaccepted_valid_not_accepted_in_old = 0
    added_examples = []
    forbidden_examples = []
    lineage_mismatch_examples = []

    for tid in species_candidates:
        new_t = new_worms_map[tid]
        pid = new_t.get("parentNameUsageID")
        if pid is None:
            n_no_parent += 1
            continue
        parent_key = str(pid)
        if parent_key not in old_worms_map:
            n_parent_missing += 1
            continue

        old_parent = old_worms_map[parent_key]
        pidx = parent_rank_index(old_parent)
        if pidx is None:
            n_bad_parent_rank += 1
            continue
        diffs = lineage_diffs_vs_parent(new_t, old_parent, pidx)
        if diffs:
            n_lineage_mismatch += 1
            if len(lineage_mismatch_examples) < LINEAGE_MISMATCH_SAMPLE_COUNT:
                lineage_mismatch_examples.append(
                    (tid, parent_key, new_t.get("scientificname"), new_t.get("rank"), diffs)
                )
            continue

        if is_accepted_record(new_t):
            if tid in forbidden:
                n_forbidden_replacement += 1
                if len(forbidden_examples) < SKIPPED_REPLACEMENT_SAMPLE_COUNT and tid in replacement_reasons:
                    uid, old_nm, vu = replacement_reasons[tid]
                    forbidden_examples.append((tid, uid, old_nm, vu))
                continue
        else:
            vu = new_t.get("valid_AphiaID")
            if vu is None:
                n_unaccepted_no_valid_in_old += 1
                continue
            anchor_key = str(vu)
            anchor = additions.get(anchor_key) or old_worms_map.get(anchor_key)
            if anchor is None:
                n_unaccepted_no_valid_in_old += 1
                continue
            if not is_species_rank(anchor) or not is_accepted_record(anchor):
                n_unaccepted_valid_not_accepted_in_old += 1
                continue

        additions[tid] = copy.deepcopy(new_t)
        added += 1
        if len(added_examples) < ADDED_SAMPLE_COUNT:
            added_examples.append(
                (
                    tid,
                    parent_key,
                    new_t.get("scientificname"),
                    new_t.get("rank"),
                    new_t.get("authority"),
                    old_parent.get("scientificname"),
                    old_parent.get("rank"),
                    new_t.get("status"),
                    new_t.get("valid_AphiaID"),
                )
            )

    print_merge_report(
        n_missing_any_rank,
        n_species_candidates,
        added,
        n_no_parent,
        n_parent_missing,
        n_bad_parent_rank,
        n_lineage_mismatch,
        n_forbidden_replacement,
        n_unaccepted_no_valid_in_old,
        n_unaccepted_valid_not_accepted_in_old,
        added_examples,
        forbidden_examples,
        lineage_mismatch_examples,
    )
    return additions


POSTGRES_BATCH_SIZE = 10000


def int_if_not_none(value):
    return int(value) if value is not None and value != "" else None


def _worms_obj_to_aphia_row(aphiaid: str, obj: dict) -> dict:
    """Same row shape as scripts/create_postgres.py."""
    info = AphiaInfo({}, {}, None, None, None)
    sync_dict(info.record, obj, RECORD_FIELDS)
    sync_dict(info.classification, obj, CLASSIFICATION_FIELDS)
    info.record["lsid"] = f"urn:lsid:marinespecies.org:taxname:{info.record['AphiaID']}"
    return {
        "id": int(aphiaid),
        "record": json.dumps(info.record),
        "classification": json.dumps(info.classification),
        "wrims": obj.get("wrims"),
        "hab": obj.get("hab"),
        "redlist_category": obj.get("redlist_category"),
        "bold_id": int_if_not_none(obj.get("bold_id")),
        "ncbi_id": int_if_not_none(obj.get("ncbi_id")),
    }


def insert_additions_into_postgres(additions: dict, table: str, schema: str = "obis", batch_size: int = POSTGRES_BATCH_SIZE):
    """Insert merge additions into an existing obis aphia table (e.g. a staging copy of production)."""
    if not additions:
        logging.info("No additions to insert into postgres")
        return

    insert_query = f"""
        INSERT INTO {schema}.{table} (
            id, record, classification, wrims, hab, redlist_category, bold_id, ncbi_id
        ) VALUES (
            %(id)s, %(record)s, %(classification)s, %(wrims)s, %(hab)s,
            %(redlist_category)s, %(bold_id)s, %(ncbi_id)s
        ) ON CONFLICT (id) DO UPDATE SET
            record = EXCLUDED.record,
            classification = EXCLUDED.classification,
            wrims = EXCLUDED.wrims,
            hab = EXCLUDED.hab,
            redlist_category = EXCLUDED.redlist_category,
            bold_id = EXCLUDED.bold_id,
            ncbi_id = EXCLUDED.ncbi_id;
    """

    conn = psycopg2.connect(
        "host='%s' dbname='%s' user='%s' password='%s' options='-c statement_timeout=%s'"
        % (
            os.getenv("DB_HOST"),
            os.getenv("DB_DB"),
            os.getenv("DB_USER"),
            os.getenv("DB_PASSWORD"),
            os.getenv("DB_TIMEOUT"),
        )
    )
    cur = conn.cursor()
    records = []
    for index, aphiaid in enumerate(additions):
        records.append(_worms_obj_to_aphia_row(aphiaid, additions[aphiaid]))
        if len(records) >= batch_size:
            psycopg2.extras.execute_batch(cur, insert_query, records)
            records = []
            logging.info("Inserted into %s.%s: %s / %s", schema, table, index + 1, len(additions))
    if records:
        psycopg2.extras.execute_batch(cur, insert_query, records)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("Inserted %s rows into %s.%s", len(additions), schema, table)


def main():
    # Before running: download OBIS and GBIF WoRMS exports, check other tables in case of decoration

    old_sources = [
        "/Volumes/acasis/worms/WoRMS_DwC-A_20250911",
        "/Volumes/acasis/worms/WoRMS_OBIS_20250911",
    ]
    old_worms_map = build_worms_map(old_sources)
    update_redlist_by_name(old_worms_map, "data/redlist.tsv")
    update_hab(old_worms_map, "/Volumes/acasis/worms/WoRMS_OBIS_HAB_20250911")
    update_wrims(old_worms_map, "/Volumes/acasis/worms/WoRMS_WRiMS_20250911")
    update_external(old_worms_map, "data/external.tsv")

    new_sources = [
        "/Volumes/acasis/worms/WoRMS_DwC-A_20260509",
        "/Volumes/acasis/worms/WoRMS_OBIS_20260509",
    ]
    new_worms_map = build_worms_map(new_sources)
    update_redlist_by_name(new_worms_map, "data/redlist.tsv")
    update_hab(new_worms_map, "/Volumes/acasis/worms/WoRMS_OBIS_HAB_20250911")
    update_wrims(new_worms_map, "/Volumes/acasis/worms/WoRMS_WRiMS_20250911")
    update_external(new_worms_map, "data/external.tsv")

    additions = merge_new_species_into_old_when_compatible(old_worms_map, new_worms_map)

    # old_worms_map.update(additions)
    # db_path = "/Volumes/acasis/worms/worms_draft_20260522.db"
    # export_to_sqlite(old_worms_map, db_path)

    insert_additions_into_postgres(additions, "aphia_staging")


if __name__ == "__main__":
    main()
