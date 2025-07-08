# aphia-sync

This is the OBIS toolset for working with WoRMS taxonomy.

## Creating taxonomy assets for the OBIS platform

The OBIS platforms needs taxonomy assets to (1) support the portal and (2) to support taxon matching in the OBIS pipeline. For the portal, a PostgreSQL table needs to be created, while taxon matching in the pipeline and for products creation uses a SQLite database with taxonomic data and parsed names. Both assets are created by merging a number of (incomplete) WoRMS exports. They are static and need to be regenerated periodically.

Additional sources are used to enhance the assets with WRiMS status, HAB status, Red List category, and external identifiers (NCBI and BOLD).

## Data ingestion

The different scripts on this repository rely on a function `aphiasync.worms.build_worms_map()`. This takes a WoRMS export folder, and generates a mapping (dict) of Aphia IDs to taxonomic information. Identifiers for all parent taxa are added to each record, as well as the valid name, and environment flags. Utility functions exist in `aphiasync.util` to add information from other sources to the map. 

### Preparing external sources

- IUCN Red List: run `Rscript scripts/redlist.R` to generate `data/redlist.tsv`.
- NCBI and BOLD IDs (BOLD currently not available): run `Rscript scripts/external.R` to generate `data/external.tsv`. Check the input folders.
- WRiMS: download WRiMS specific WoRMS export.
- HAB: download HAB specific WoRMS export.

## PostgreSQL table

To create a PostgreSQL table in the OBIS database, use `scripts/create_pg_table.py`. Make sure to update the table name and data sources.

## SQLite database

To create the SQLite database for taxon matching, use `scripts/parse_worms_to_sqlite`. This uses <https://github.com/gnames/gnparser> to add the canonical name for each record.

## Taxon matching

To match a list of names against the SQLite database, use the `aphiasync.sqlite.match()` function from this package. Make sure that the `WORMS_DB_PATH` environment variable points to the SQLite database. This will parse the input names and try to find a matching name in the database. If multiple matches are found, the function will use the authorship to find a single match. If there are still multiple possible matches, none are returned.

```python
from aphiasync.sqlite import match
import pprint


os.environ["WORMS_DB_PATH"] = "worms.db"

names = [
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
]

matched = match(names)
print(pprint.pp(matched, indent=2, depth=2))
```

```json
{ 'Abra (Abra) Lamarck, 1818': {'aphiaid': 1660648, 'record': {...}},
  'Abra alva': {'aphiaid': None},
  'Paridotea munda Nunomura, 1988': {'aphiaid': 325572, 'record': {...}},
  'Larus dominicanus dominicanus': {'aphiaid': None},
  'Abra alba (W. Wood 1802)': {'aphiaid': 141433, 'record': {...}},
  'Abra alba W. Wood 1802': {'aphiaid': None},
  'Skeletonema menzellii': {'aphiaid': None},
  'Paridotea munda Hale, 1924': {'aphiaid': 257361, 'record': {...}},
  'Paridotea munda': {'aphiaid': None},
  'Abra Lamarck, 1818': {'aphiaid': 138474, 'record': {...}},
  'Ulva lactuca': {'aphiaid': 145984, 'record': {...}},
  'Abra alba': {'aphiaid': 141433, 'record': {...}}}
```
