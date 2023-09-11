"""
Abstraction layer of PyMongo with auto-reconnect and collection initialization support.
"""
__version__ = "0.1.14"

# pymongo shortcuts
import pymongo

from . import types, mongo_errors, engine, schema
from .engine import Engine
from .schema import Collection, Database
from .types import Executor, Index, CollectionDefinition, Document

ASCENDING = pymongo.ASCENDING
DESCENDING = pymongo.DESCENDING
