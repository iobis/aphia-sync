import copy
import csv
import re
import os


def cleanup_dict(o):
    """Remove properties that should not be taken into account for change detection."""
    o_copy = copy.deepcopy(o)
    if o_copy is not None:
        o_copy.pop("modified", None)
    if o_copy is not None:
        o_copy.pop("citation", None)
    return o_copy


def update_hab(worms_map, export_path):
    with open(os.path.join(export_path, "taxon.txt")) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            taxon_id = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["taxonID"]).group(1)
            if taxon_id in worms_map:
                worms_map[taxon_id]["hab"] = True


def update_wrims(worms_map, export_path):
    with open(os.path.join(export_path, "taxon.txt")) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            taxon_id = re.search("urn:lsid:marinespecies.org:taxname:([0-9]+)", row["taxonID"]).group(1)
            if taxon_id in worms_map:
                worms_map[taxon_id]["wrims"] = True


def update_redlist(worms_map, export_path):
    with open(export_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            taxon_id = row["id"]
            redlist_category = row["redlist_category"]
            if taxon_id in worms_map:
                worms_map[taxon_id]["redlist_category"] = redlist_category


def update_redlist_by_name(worms_map, export_path):
    with open(export_path) as csvfile:
        redlist_map = {}
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            redlist_map[row["species"]] = row["category"]
        for taxonid in worms_map:
            if worms_map[taxonid]["scientificname"] in redlist_map:
                worms_map[taxonid]["redlist_category"] = redlist_map[worms_map[taxonid]["scientificname"]]

def update_external(worms_map, export_path):
    with open(export_path) as csvfile:
        reader = csv.DictReader(csvfile, delimiter="\t")
        for row in reader:
            taxon_id = row["id"]
            if taxon_id in worms_map:
                if row["ncbi_id"]:
                    worms_map[taxon_id]["ncbi_id"] = row["ncbi_id"]
                if row["bold_id"]:
                    worms_map[taxon_id]["bold_id"] = row["bold_id"]
