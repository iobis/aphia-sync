from dotenv import load_dotenv
from aphiasync import bulk_update
from aphiasync.obisconnector import OBISConnector
from aphiasync.worms import build_worms_map


load_dotenv()

# TODO: CHECK APHIA TABLE in env!!!
# bulk_update(sync=True, fill=False, aphiaids=["279822"])
# bulk_update(sync=True, fill=False, skip=1055754)
bulk_update(sync=True, fill=False, aphiaids=["301897"], dry_run=False)
