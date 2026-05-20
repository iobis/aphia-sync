import gnparser
import json
import os
import sqlite3


def sanitize_name(name: str) -> str:
    return name.replace("#", "").replace("_", " ").strip()


def match(names: list[str]):

    # sanitize

    original_names = list(names)
    sanitized_names = [sanitize_name(name) for name in original_names]

    # get all distinct names

    distinct_names = list(set(sanitized_names))

    # parse names into canonical and authorship

    parsed_by_sanitized = {}

    for name in distinct_names:
        parsed_str = gnparser.parse_to_string(name, "compact", None, 1, 1)
        parsed = json.loads(parsed_str)
        if parsed.get("parsed"):
            canonical = parsed.get("canonical", None).get("full", None)
            authorship = parsed.get("authorship", {}).get("normalized", None)
            parsed_by_sanitized[name] = (canonical, authorship)
        else:
            parsed_by_sanitized[name] = (None, None)

    # get all matches for each canonical name

    con = sqlite3.connect(os.getenv("WORMS_DB_PATH"))
    con.row_factory = sqlite3.Row
    cur = con.cursor()

    canonicals = list(set(c for c, _ in parsed_by_sanitized.values() if c is not None))
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

    for name in distinct_names:
        canonical, authorship = parsed_by_sanitized[name]
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

    # get valid matches for distinct sanitized names

    taxa_by_sanitized = {}

    for i in range(len(name_matches)):
        name = distinct_names[i]
        canonical, _ = parsed_by_sanitized[name]
        matches = name_matches[i]
        if matches is not None and len(matches) == 1:
            taxa_by_sanitized[name] = {
                "canonical": canonical,
                "aphiaid": int(matches[0]["aphiaid"]),
            }
        else:
            valid_aphiaids = list(set([match["valid_aphiaid"] for match in matches]))
            if len(valid_aphiaids) == 1:
                taxa_by_sanitized[name] = {
                    "canonical": canonical,
                    "aphiaid": int(valid_aphiaids[0]) if valid_aphiaids[0] is not None else None,
                }
            else:
                taxa_by_sanitized[name] = {
                    "canonical": canonical,
                    "aphiaid": None,
                }

    # add records from aphiaid

    for taxon in taxa_by_sanitized:
        if taxa_by_sanitized[taxon]["aphiaid"] is not None:
            cur.execute("select * from parsed where aphiaid = ?", (taxa_by_sanitized[taxon]["aphiaid"],))
            res = cur.fetchone()
            if res is not None:
                record = json.loads(res["record"])
                taxa_by_sanitized[taxon]["record"] = record

    con.close()

    taxa = {}
    for original, sanitized in zip(original_names, sanitized_names):
        taxa[original] = {
            "sanitized": sanitized,
            **taxa_by_sanitized.get(
                sanitized,
                {"canonical": None, "aphiaid": None},
            ),
        }

    return taxa