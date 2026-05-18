"""WoRMS taxonomy sync and name matching."""

__version__ = "0.3.0"

# Lazy re-exports so `from aphiasync.sqlite import match` does not load sync/Postgres deps.
_LAZY_EXPORTS = frozenset({
    "get_obis_connector",
    "scan",
    "sync_dict",
    "do_fill",
    "do_sync",
    "bulk_update",
})


def __getattr__(name: str):
    if name in _LAZY_EXPORTS:
        from aphiasync import sync

        return getattr(sync, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(list(globals().keys()) + list(_LAZY_EXPORTS))
