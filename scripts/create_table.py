from dotenv import load_dotenv
from aphiasync.aphiainfo import AphiaInfo
from aphiasync import sync_dict
from dotenv import load_dotenv
from aphiasync.worms import build_worms_map, CLASSIFICATION_FIELDS, RECORD_FIELDS
from aphiasync.util import update_hab, update_wrims, update_redlist, update_external
import json
import os
import psycopg2


load_dotenv()
APHIA_TABLE = "aphia_20250310"
BATCH_SIZE = 10000
conn = psycopg2.connect(
    "host='%s' dbname='%s' user='%s' password='%s' options='-c statement_timeout=%s'" %
    (os.getenv("DB_HOST"), os.getenv("DB_DB"), os.getenv("DB_USER"), os.getenv("DB_PASSWORD"), os.getenv("DB_TIMEOUT")))
# conn.autocommit = True
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)


worms_map_1 = build_worms_map("/Users/pieter/Desktop/temp/WoRMS_OBIS")
worms_map_2 = build_worms_map("/Users/pieter/Desktop/temp/WoRMS_DwC-A")

for key in worms_map_2:
    worms_map_1[key] = worms_map_2[key]

update_hab(worms_map_1, "/Users/pieter/Desktop/temp/WoRMS_OBIS_HAB")
update_wrims(worms_map_1, "/Users/pieter/Desktop/temp/WoRMS_WRiMS")
update_redlist(worms_map_1, "/Users/pieter/Desktop/temp/redlist.tsv")
update_external(worms_map_1, "/Users/pieter/Desktop/temp/external.tsv")


def int_if_not_none(value):
    return int(value) if value is not None and value != "" else None


insert_query = f"""
    INSERT INTO obis.{APHIA_TABLE} (
        id, record, classification, wrims, hab, redlist_category, bold_id, ncbi_id
    ) VALUES (
        %(id)s, %(record)s, %(classification)s, %(wrims)s, %(hab)s, %(redlist_category)s, %(bold_id)s, %(ncbi_id)s
    ) ON CONFLICT (id) DO UPDATE SET
    record = EXCLUDED.record,
    classification = EXCLUDED.classification,
    wrims = EXCLUDED.wrims,
    hab = EXCLUDED.hab,
    redlist_category = EXCLUDED.redlist_category,
    bold_id = EXCLUDED.bold_id,
    ncbi_id = EXCLUDED.ncbi_id;
    """

records = []

for index, aphiaid in enumerate(worms_map_1):
    print(f"Inserting into {APHIA_TABLE}: {aphiaid} ({index} / {len(worms_map_1)})")
    info = AphiaInfo({}, {}, None, None, None)
    obj = worms_map_1[aphiaid]
    sync_dict(info.record, obj, RECORD_FIELDS)
    sync_dict(info.classification, obj, CLASSIFICATION_FIELDS)
    info.record["lsid"] = f"urn:lsid:marinespecies.org:taxname:{info.record['AphiaID']}"

    record = {
        "id": int(aphiaid),
        "record": json.dumps(info.record),
        "classification": json.dumps(info.classification),
        "wrims": obj.get("wrims", None),
        "hab": obj.get("hab", None),
        "redlist_category": obj.get("redlist_category", None),
        # "redlist_id": None,  # TODO: handle removal of redlist_id
        "bold_id": int_if_not_none(obj.get("bold_id", None)),
        "ncbi_id": int_if_not_none(obj.get("ncbi_id", None))
    }
    # cur.execute(insert_query, record)
    records.append(record)
    if len(records) > BATCH_SIZE:
        psycopg2.extras.execute_batch(cur, insert_query, records)
        records = []

if len(records) > 0:
    psycopg2.extras.execute_batch(cur, insert_query, records)

conn.commit()

cur.execute(f"""
CREATE INDEX ix_{APHIA_TABLE}_expr ON obis.{APHIA_TABLE} USING btree (((record ->> 'scientificname'::text)));
CREATE INDEX ix_{APHIA_TABLE}_lower ON obis.{APHIA_TABLE} USING btree (lower((record ->> 'scientificname'::text)));
CREATE INDEX ix_{APHIA_TABLE}_boldid ON obis.{APHIA_TABLE} USING btree (bold_id);
CREATE INDEX ix_{APHIA_TABLE}_classid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'classid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_divisionid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'divisionid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_domainid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'domainid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_familyid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'familyid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_formaid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'formaid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_genusid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'genusid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_gigaclassid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'gigaclassid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_infraclassid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'infraclassid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_infrakingdomid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'infrakingdomid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_infraorderid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'infraorderid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_infraphylumid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'infraphylumid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_kingdomid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'kingdomid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_megaclassid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'megaclassid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_ncbiid ON obis.{APHIA_TABLE} USING btree (ncbi_id);
CREATE INDEX ix_{APHIA_TABLE}_orderid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'orderid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_parvorderid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'parvorderid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_parvphylumid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'parvphylumid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_phylumdivisionid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'phylum (division)id'::text)));
CREATE INDEX ix_{APHIA_TABLE}_phylumid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'phylumid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_redlist_id ON obis.{APHIA_TABLE} USING btree (redlist_id);
CREATE INDEX ix_{APHIA_TABLE}_scientificname_2 ON obis.{APHIA_TABLE} USING btree (((record ->> 'scientificname'::text)));
CREATE INDEX ix_{APHIA_TABLE}_sectionid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'sectionid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_speciesid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'speciesid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subclassid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subclassid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subdivisionid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subdivisionid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subfamilyid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subfamilyid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subformaid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subformaid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subgenusid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subgenusid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subkingdomid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subkingdomid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_suborderid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'suborderid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subphylumid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subphylumid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subphylumsubdivisionid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subphylum (subdivision)id'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subsectionid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subsectionid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subspeciesid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subspeciesid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subterclassid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subterclassid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subtribeid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subtribeid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_subvarietyid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'subvarietyid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_superclassid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'superclassid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_superdomainid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'superdomainid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_superfamilyid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'superfamilyid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_superorderid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'superorderid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_supertribeid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'supertribeid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_tribeid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'tribeid'::text)));
CREATE INDEX ix_{APHIA_TABLE}_varietyid ON obis.{APHIA_TABLE} USING btree (((classification ->> 'varietyid'::text)));
""")

conn.commit();
cur.close()
conn.close()
