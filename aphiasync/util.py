import copy


def cleanup_dict(o):
    """Remove properties that should not be taken into account for change detection."""
    o_copy = copy.deepcopy(o)
    if o_copy is not None:
        o_copy.pop("modified", None)
    if o_copy is not None:
        o_copy.pop("citation", None)
    return o_copy
