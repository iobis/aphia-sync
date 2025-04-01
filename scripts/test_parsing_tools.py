import pyworms
import sqlite3
import logging
from termcolor import colored
from gnparser import parse_to_string
import json


logging.basicConfig(level=logging.INFO)


def match_with_worms(name: str):
    matches = pyworms.aphiaRecordsByMatchNames(name, False)[0]
    return [{
        "aphiaid": match["AphiaID"],
        "scientificname": match["scientificname"],
        "match_type": match["match_type"]
    } for match in matches if match["match_type"].startswith("exact")]


def match_with_sqlite(name: str):
    parsed_str = parse_to_string(name, "compact", None, 1, 1)
    parsed = json.loads(parsed_str)
    canonical = parsed.get("canonical", None).get("full", None)
    authorship = parsed.get("authorship", {}).get("normalized", None)

    con = sqlite3.connect("data/worms_parsed.db")
    con.row_factory = sqlite3.Row
    cur = con.cursor()
    cur.execute("SELECT * FROM parsed WHERE canonical = ?", (canonical,))
    matches = cur.fetchall()
    con.close()

    all_results = [{
        "aphiaid": match_dict["aphiaid"],
        "scientificname": match_dict["canonical"],
        "authorship": match_dict["authorship"],
    } for match_dict in [dict(match) for match in matches]]

    if len(all_results) > 1:
        logging.info(colored(f"Found multiple matches: {all_results}", "red"))
        results = [result for result in all_results if result["authorship"] == authorship]
    else:
        results = all_results

    return results


def check_names():

    for name in [
        "Abra alba",
        "Abra alba (W. Wood 1802)",
        "Abra alba W. Wood 1802",
        "Abra (Abra) Lamarck, 1818",
        "Abra Lamarck, 1818",
        "Abra alva",
        "Larus dominicanus dominicanus",
        "Skeletonema menzellii",
        "Paridotea munda",
        "Paridotea munda Hale, 1924",
        "Paridotea munda Nunomura, 1988",
        "Ulva lactuca"
    ]:

        logging.info(colored(f"{name}", "green"))
        matches_worms = match_with_worms(name)
        logging.info(f"WoRMS: {matches_worms}")
        matches_sqlite = match_with_sqlite(name)
        logging.info(f"local: {matches_sqlite}")


check_names()
