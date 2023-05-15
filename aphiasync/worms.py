import csv
import os
import re


RANKS = ["superdomain", "domain", "kingdom", "subkingdom", "infrakingdom", "superphylum", "phylum", "phylum (division)", "subphylum", "subphylum (subdivision)", "infraphylum", "parvphylum", "gigaclass", "megaclass", "superclass", "class", "subclass", "infraclass", "subterclass", "superorder", "order", "suborder", "infraorder", "parvorder", "section", "subsection", "superfamily", "epifamily", "family", "subfamily", "supertribe", "tribe", "subtribe", "genus", "subgenus", "series", "species", "subspecies", "natio", "variety", "subvariety", "forma", "subforma", "mutatio"]
RANK_FIELDS = RANKS + [rank + "id" for rank in RANKS]
CLASSIFICATION_FIELDS = RANK_FIELDS + ["parentNameUsage", "parentNameUsageID"]
RECORD_FIELDS = ["taxonRankID", "isBrackish", "valid_authority", "modified", "lsid", "genus", "AphiaID", "citation", "kingdom", "isFreshwater", "isExtinct", "class", "status", "valid_name", "url", "match_type", "isTerrestrial", "family", "rank", "isMarine", "order", "scientificname", "unacceptreason", "phylum", "parentNameUsageID", "authority", "valid_AphiaID"]


def build_worms_map():

    worms_map = dict()
    parents_map = {rank: {} for rank in RANKS}

    # read taxon table and create parent-child map

    with open(os.path.join(os.getenv("WORMS_EXPORT"), "taxon.txt")) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:

            obj = dict()
            obj["scientificname"] = row["scientificName"]
            obj["citation"] = row["bibliographicCitation"]
            obj["modified"] = row["modified"]
            obj["authority"] = row["scientificNameAuthorship"] if row["scientificNameAuthorship"] != "" else None
            obj["parentNameUsage"] = row["parentNameUsage"]
            obj["status"] = row["taxonomicStatus"]
            taxon_rank = row["taxonRank"] if row["taxonRank"] != "" else None
            obj["rank"] = taxon_rank
            taxon_id = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["taxonID"]).group(1)
            obj["url"] = f"https://www.marinespecies.org/aphia.php?p=taxdetails&id={taxon_id}"
            obj["AphiaID"] = int(taxon_id)
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
                parent_id = parent_match.group(1)
                obj["parentNameUsageID"] = int(parent_id)
                if parent_id in parents_map[clean_rank]:
                    parents_map[clean_rank][parent_id].append(taxon_id)
                else:
                    parents_map[clean_rank][parent_id] = [taxon_id]

            worms_map[taxon_id] = obj

    # populate direct children (highest rank first)

    for rank in RANKS:
        for parent_id, child_ids in parents_map[rank].items():
            for r in RANKS:
                if parent_id in worms_map and r in worms_map[parent_id]:
                    for child_id in child_ids:
                        worms_map[child_id][r] = worms_map[parent_id][r]
                        worms_map[child_id][r + "id"] = worms_map[parent_id][r + "id"]

    # populate valid name and authority

    for taxon_id, obj in worms_map.items():
        if "valid_AphiaID" in obj:
            valid_id = str(obj["valid_AphiaID"])
            if valid_id in worms_map:
                obj["valid_name"] = worms_map[valid_id]["scientificname"] if worms_map[valid_id]["scientificname"] != "" else None
                obj["valid_authority"] = worms_map[valid_id]["authority"] if worms_map[valid_id]["authority"] != "" else None

    # add flags

    with open(os.path.join(os.getenv("WORMS_EXPORT"), "speciesprofile.txt")) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            taxon_id = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["taxonID"]).group(1)
            worms_map[taxon_id]["isMarine"] = int(row["isMarine"]) if row["isMarine"] != "" else None
            worms_map[taxon_id]["isBrackish"] = int(row["isBrackish"]) if row["isBrackish"] != "" else None
            worms_map[taxon_id]["isFreshwater"] = int(row["isFreshwater"]) if row["isFreshwater"] != "" else None
            worms_map[taxon_id]["isTerrestrial"] = int(row["isTerrestrial"]) if row["isTerrestrial"] != "" else None
            worms_map[taxon_id]["isExtinct"] = int(row["isExtinct"]) if row["isExtinct"] != "" else None

    return worms_map
