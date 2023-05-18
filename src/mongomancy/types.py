import abc
import datetime as dt
import logging
import multiprocessing
import sys
import threading
import typing
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import (
    OrderedDict as OrderedDictType,
    List,
    Tuple,
    Sequence,
    Union,
    Optional,
    Mapping,
    Dict,
    Any,
    Iterable,
    Callable,
)

import pymongo.client_session
import pymongo.database
import pymongo.results
from bson import ObjectId
from pymongo.command_cursor import CommandCursor

__all__ = (
    "Bson",
    "BsonDict",
    "BsonList",
    "Index",
    "Document",
    "CollectionDefinition",
    "CollectionContainer",
    "Executor",
    "CommandCursor",
    "SemaphoreTower",
)

Bson = Union[None, int, float, bool, str, ObjectId, dt.datetime, Sequence["Bson"], Mapping[str, "Bson"]]
BsonDict = Mapping[str, Bson]
BsonList = Sequence[Bson]

OrderedFields = OrderedDictType[str, Union[str, int]]
if sys.version_info >= (3, 7):
    OrderedFields = Dict[str, Union[str, int]]

OrderedPairs = Union[OrderedFields, Sequence[Tuple[str, Union[str, int]]]]

METHOD = typing.Literal[
    "find",
    "find_one",
    "find_one_and_update",
    "update_one",
    "update_many",
    "insert_one",
    "insert_many",
    "delete_one",
    "delete_many",
    "aggregate",
]


class CollectionContainer(typing.Protocol):
    dialect_entity: pymongo.collection.Collection


class SemaphoreTower:
    __slots__ = ("multiprocess", "thread", "timeout", "logger")
    multiprocess: multiprocessing.Semaphore
    thread: threading.Semaphore
    timeout: Optional[float]
    logger: Optional[logging.Logger]

    def __init__(
        self, value: int = 1, timeout: Optional[float] = None, logger: Optional[logging.Logger] = None
    ) -> None:
        """
        Helper to make multiple semaphores work as one critical section

        :param value: semaphore init value
        :param timeout: timeout is used for EACH semaphore in cascade, so result timeout is 2xtimeout
        :param logger: give me info about what happens here
        """
        value = min(value or 1, 1)
        self.logger = logger
        self.timeout = timeout
        self.multiprocess = multiprocessing.Semaphore(value)
        self.thread = threading.Semaphore(value)

    def __enter__(self):
        if self.logger:
            self.logger.debug("waiting for semaphore tower")
        self.multiprocess.acquire(timeout=self.timeout)
        self.thread.acquire(timeout=self.timeout)
        if self.logger:
            self.logger.debug("entering semaphore tower context")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.thread.release()
        self.multiprocess.release()
        if self.logger:
            self.logger.debug("released semaphore tower context")


@dataclass(init=False)
class Index:
    """Collection Index data container"""

    fields: OrderedDict[str, Union[str, int]]
    name: Optional[str]
    unique: Optional[bool]

    __slots__ = ("fields", "name", "unique")

    def __init__(self, fields: OrderedPairs, name: Optional[str] = None, unique: Optional[bool] = False) -> None:
        self.fields = OrderedDict(fields)
        self.name = name
        self.unique = unique

    def field_for_mongo(self) -> List[Tuple[str, Union[str, int]]]:
        """Format out FIELDS as order of tuples"""
        return [(k, v) for k, v in self.fields.items()]


@dataclass(slots=True)
class Document:
    """
    Abstraction of a MongoDB document where unique_key is one or more elements clearly identifying one document

    Example:
        Document(unique_key={"color_name": "red"}, data={"color_name": "red", "color_hex": "#ff0000"})
    """

    unique_key: Optional[BsonDict]
    data: Bson


@dataclass(slots=True)
class CollectionDefinition:
    """
    Collection Definition of NAME and indexed field
    """

    name: str
    indices: Sequence[Index] = field(default_factory=tuple)
    default_docs: Sequence[Document] = field(default_factory=tuple)

    def fill_index_names(self):
        """
        Do through indices and fill missing `index.NAME`s. Affects internal state of `self.indices` !
        """
        for index in self.indices:
            if index.name is None:
                index.name = f"{'ux' if index.unique else 'ix'}__{self.name or ''}__{'_'.join(index.fields)}"


class Executor(metaclass=abc.ABCMeta):
    """
    Reconnect-able interface of MongoDB mongo_driver that allows to execute commands like:
    `find`, `insert_one`, `update_many`, `delete_one`.
    """

    __slots__ = ()

    @abc.abstractmethod
    def start_session(
        self,
        causal_consistency: Optional[bool] = None,
        default_transaction_options: Optional[pymongo.client_session.TransactionOptions] = None,
        snapshot: Optional[bool] = False,
    ) -> pymongo.client_session.ClientSession:
        ...

    @abc.abstractmethod
    def find_one(
        self, collection: CollectionContainer, where: Optional[BsonDict], *args, **kwargs
    ) -> Optional[Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def find(
        self, collection: CollectionContainer, where: Optional[BsonDict], *args, **kwargs
    ) -> pymongo.cursor.Cursor:
        ...

    @abc.abstractmethod
    def find_one_and_update(
        self, collection: CollectionContainer, where: BsonDict, changes: BsonDict | BsonList, *args, **kwargs
    ) -> Optional[BsonDict]:
        ...

    @abc.abstractmethod
    def update_one(
        self, collection: CollectionContainer, where: Optional[BsonDict], changes: BsonDict | BsonList, *args, **kwargs
    ) -> pymongo.results.UpdateResult:
        ...

    @abc.abstractmethod
    def update_many(
        self, collection: CollectionContainer, where: Optional[BsonDict], changes: BsonDict | BsonList, *args, **kwargs
    ) -> pymongo.results.UpdateResult:
        ...

    @abc.abstractmethod
    def insert_one(
        self, collection: CollectionContainer, document: Optional[BsonDict], *args, **kwargs
    ) -> pymongo.results.InsertOneResult:
        ...

    @abc.abstractmethod
    def insert_many(
        self, collection: CollectionContainer, documents: Iterable[BsonDict], *args, **kwargs
    ) -> pymongo.results.InsertManyResult:
        ...

    @abc.abstractmethod
    def delete_one(
        self, collection: CollectionContainer, where: Optional[BsonDict], *args, **kwargs
    ) -> pymongo.results.DeleteResult:
        ...

    @abc.abstractmethod
    def delete_many(
        self, collection: CollectionContainer, where: Optional[BsonDict], *args, **kwargs
    ) -> pymongo.results.DeleteResult:
        ...

    @abc.abstractmethod
    def aggregate(
        self, collection: CollectionContainer, pipeline: BsonList, *args, **kwargs
    ) -> pymongo.command_cursor.CommandCursor:
        ...

    @abc.abstractmethod
    def register_hook(self, reconnect_hook: Callable[["Executor"], None]) -> None:
        ...

    @abc.abstractmethod
    def reconnect(self) -> None:
        ...

    @abc.abstractmethod
    def dispose(self) -> None:
        """
        Cleanup client resources and disconnect from MongoDB.
        """
        ...

    @abc.abstractmethod
    def ping(self, database: Optional[str] = None) -> bool:
        ...

    @abc.abstractmethod
    def get_database(self, name: str) -> pymongo.database.Database:
        ...

    @abc.abstractmethod
    def drop_database(self, name: str) -> None:
        ...
