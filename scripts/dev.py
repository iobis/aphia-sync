from dotenv import load_dotenv
from aphiasync import bulk_update
from aphiasync.obisconnector import OBISConnector
from aphiasync.worms import build_worms_map


load_dotenv()

# TODO: for bulk update: check if records are getting smaller by updating (distributions??)
# TODO: CHECK APHIA TABLE in env!!!
bulk_update(sync=True, fill=False)


# obis_connector = OBISConnector()
# for id in ids:
#     obis_connector.check(id)
