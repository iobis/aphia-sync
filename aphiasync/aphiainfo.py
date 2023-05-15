import json
from aphiasync.util import cleanup_dict


class AphiaInfo:

    def __init__(self, record, classification, distribution, bold_id, ncbi_id):
        self.record = record
        self.classification = classification
        self.distribution = distribution
        self.bold_id = bold_id
        self.ncbi_id = ncbi_id

    def __str__(self):
        return json.dumps({
            "record": cleanup_dict(self.record),
            "classification": cleanup_dict(self.classification),
            "distribution": self.distribution,
            "bold_id": self.bold_id,
            "ncbi_id": self.ncbi_id
        }, sort_keys=True)

    def __eq__(self, other):
        return str(self) == str(other)
