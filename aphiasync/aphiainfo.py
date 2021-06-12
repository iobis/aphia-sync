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
        return json.dumps([
            cleanup_dict(self.record),
            cleanup_dict(self.classification),
            self.distribution,
            self.bold_id,
            self.ncbi_id
        ], sort_keys=True)

    def __eq__(self, other):
        return str(self) == str(other)
