import gnparser
import json
import sqlite3
import os


def sanitize_name(name: str) -> str:
    return name.replace("#", "")


def match(names: list[str]):

    # sanitize

    names = [sanitize_name(name) for name in names]

    # get all distinct names

    distinct_names = list(set(names))

    # parse names into canonical and authorship

    parsed_names = []

    for name in distinct_names:
        parsed_str = gnparser.parse_to_string(name, "compact", None, 1, 1)
        parsed = json.loads(parsed_str)
        if parsed.get("parsed"):
            canonical = parsed.get("canonical", None).get("full", None)
            authorship = parsed.get("authorship", {}).get("normalized", None)
            parsed_names.append((canonical, authorship))
        else:
            parsed_names.append((None, None))

    # get all matches for each canonical name

    con = sqlite3.connect(os.getenv("WORMS_DB_PATH"))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    canonicals = list(set([name[0] for name in parsed_names if name[0] is not None]))
    placeholders = ",".join("?" * len(canonicals))
    
    cur.execute(f"select * from parsed where canonical in ({placeholders})", canonicals)
    matches = cur.fetchall()
    canonicals_map = {}

    for row in matches:
        canonical = row["canonical"]
        if canonical not in canonicals_map:
            canonicals_map[canonical] = []
        canonicals_map[canonical].append({
            "aphiaid": row["aphiaid"],
            "scientificname": row["canonical"],
            "authorship": row["authorship"],
            "valid_aphiaid": row["valid_aphiaid"]
        })

    # perform matching on both canonical name and authorship,
    # this may still include multiple matches if authorship is not known

    name_matches = []

    for name_pair in parsed_names:
        canonical = name_pair[0]
        authorship = name_pair[1]
        if canonical is not None:
            if canonical in canonicals_map:
                matches = canonicals_map[canonical]
                if authorship is not None:
                    matches = [match for match in matches if match["authorship"] == authorship]
            else:
                matches = []
        else:
            matches = []
        name_matches.append(matches)

    assert len(name_matches) == len(distinct_names)

    # get valid matches for distinct names

    taxa = {}

    for i in range(len(name_matches)):
        name = distinct_names[i]
        matches = name_matches[i]
        if matches is not None and len(matches) == 1:
            taxa[name] = {
                "aphiaid": int(matches[0]["aphiaid"])
            }
        else:
            valid_aphiaids = list(set([match["valid_aphiaid"] for match in matches]))
            if len(valid_aphiaids) == 1:
                taxa[name] = {
                    "aphiaid": int(valid_aphiaids[0]) if valid_aphiaids[0] is not None else None
                }
            else:
                taxa[name] = {
                    "aphiaid": None
                }
    
    # add records from aphiaid

    for taxon in taxa:
        if taxa[taxon]["aphiaid"] is not None:
            cur.execute("select * from parsed where aphiaid = ?", (taxa[taxon]["aphiaid"],))
            res = cur.fetchone()
            if res is not None:
                record = json.loads(res["record"])
                taxa[taxon]["record"] = record

    con.close()

    print("ok")
