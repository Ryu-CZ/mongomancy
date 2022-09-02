"""
PyMongo abstraction layer with auto-reconnect and collection initialization support.
"""
__version__ = "0.0.2"

# pymongo shortcuts
import pymongo

from . import (
    types,
    mongo_errors,
    engine,
    schema,
)
from .engine import Engine
from .schema import Collection, Database
from .types import (
    Index,
    CollectionDefinition,
    Document,
)

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING
