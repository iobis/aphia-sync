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


