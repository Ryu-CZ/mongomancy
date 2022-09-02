import logging
import time
import traceback
from dataclasses import dataclass
from typing import (
    List, Tuple, Union, TypeVar, Mapping, Any, Optional, Dict, Iterable
)

import pymongo
import pymongo.database
import pymongo.results
import pymongo.typings
from pymongo.errors import PyMongoError

from . import (
    types,
)

__all__ = (
    "Collection",
    "Database",
    "DocumentType",
)

LoggerType = Union[logging.Logger, logging.LoggerAdapter]
DocumentType = TypeVar("DocumentType", bound=Mapping[str, Any])


@dataclass(slots=True)
class Collection:
    """
    Abstraction of existing collection.
    Wraps `pymongo.collection.Collection` for easier reconnect.
    """
    mongo_collection: pymongo.collection.Collection
    engine: types.Executor

    @property
    def name(self) -> str:
        return self.mongo_collection.name

    def find_one(
            self,
            where: Optional[types.BsonDict],
            *args,
            **kwargs,
    ) -> Optional[DocumentType]:
        return self.engine.find_one(self.mongo_collection, where, *args, **kwargs)

    def find(
            self,
            where: Optional[types.BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.cursor.Cursor[types.BsonDict]:
        return self.engine.find(self.mongo_collection, where, *args, **kwargs)

    def find_one_and_update(
            self,
            where: types.BsonDict,
            changes: types.BsonDict | types.BsonList,
            *args,
            **kwargs,
    ) -> Optional[types.BsonDict]:
        return self.engine.find_one_and_update(self.mongo_collection, where, changes, *args, **kwargs)

    def update_one(
            self,
            where: Optional[types.BsonDict],
            changes: types.BsonDict | types.BsonList,
            *args,
            **kwargs,
    ) -> pymongo.results.UpdateResult:
        return self.engine.update_one(self.mongo_collection, where, changes, *args, **kwargs)

    def update_many(
            self,
            where: Optional[types.BsonDict],
            changes: types.BsonDict | types.BsonList,
            *args,
            **kwargs,
    ) -> pymongo.results.UpdateResult:
        return self.engine.update_many(self.mongo_collection, where, changes, *args, **kwargs)

    def insert_one(
            self,
            document: types.BsonDict,
            *args,
            **kwargs,
    ) -> pymongo.results.InsertOneResult:
        return self.engine.insert_one(self.mongo_collection, document, *args, **kwargs)

    def insert_many(
            self,
            documents: Iterable[types.BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.InsertManyResult:
        return self.engine.insert_many(self.mongo_collection, documents, *args, **kwargs)

    def delete_one(
            self,
            where: Optional[types.BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.DeleteResult:
        return self.engine.delete_one(self.mongo_collection, where, *args, **kwargs)

    def delete_many(
            self,
            where: Optional[types.BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.DeleteResult:
        return self.engine.delete_many(self.mongo_collection, where, *args, **kwargs)


class Database:
    """
    Database abstraction with reconnect support

    Example of usage: ..
        engine = Engine("localhost", 27017)
        logger = logging.getLogger(__name__)
        db = Database(engine=engine, logger=logger)
        game = CollectionDefinition(name="game", indices=[Index(fields={"game_id": 1})])
        player = CollectionDefinition(name="player", indices=[Index(fields={"player_id": 1}, unique=True)])
        db.add_definition(game)
        db.add_definition(player)
        db.create_all()
        db["game"].find_one({"game_id": 1, "name": "game 1"})

    """
    engine: types.Executor
    topology: List[types.CollectionDefinition]
    _database: pymongo.database.Database
    _collections: Dict[str, Collection]
    logger: LoggerType

    __slots__ = (
        "engine",
        "topology",
        "_database",
        "_collections",
        "logger",
    )

    def __init__(
            self,
            name: str,
            logger: LoggerType,
            engine: types.Executor,
            *definitions: types.CollectionDefinition,
    ) -> None:
        self._collections = {}
        self.topology = []
        self.logger = logger
        self.engine = engine
        self._database = engine.get_database(name)
        self.extend(*definitions)
        self.engine.register_hook(reconnect_hook=self.invalidate_cache_hook)

    def __getitem__(self, item: str) -> Collection:
        if item not in self._collections:
            raise KeyError(f"collection {item!r} not found")
        return self._collections[item]

    def __contains__(self, item):
        return item in self._collections

    def __getattr__(self, item):
        if item in self._collections:
            return self._collections[item]
        return object.__getattr__(self, item)

    def __str__(self) -> str:
        return f"{type(self).__qualname__}(name={self.name!r})"

    @property
    def name(self) -> str:
        return self._database.name

    def invalidate_cache_hook(self, source: types.Executor) -> None:
        """
        Hook for handling reconnect event.
        Changes internal reference to pymongo database `self._database` together with
        contained `mongo_collection` of each `self._collections`.
        """
        self._database = source.get_database(self.name)
        self.logger.debug(f"{self}.invalidate_cache_hook - switched _database")
        for collection in self._collections.values():
            collection.mongo_collection = self._database[collection.name]
        self.logger.debug(f"{self}.invalidate_cache_hook - switch _collections bindings")

    def get_collection(self, name: str) -> Collection:
        """
        Get collection by name from existing `self._collections`.

        :param name: unique name of collection
        :return: instance of Collection or raise errors
        :raises KeyError: if there is no collection of that name
        """
        if name not in self._collections:
            raise KeyError(f"collection {name!r} not found")
        return self._collections[name]

    def extend(self, *new_definitions: types.CollectionDefinition) -> None:
        """
        Add new collection definitions into this `topology`.

        :param new_definitions: add these collection definitions
        """
        self.topology.extend(new_definitions)

    def create_all(self, skip_existing: bool = True) -> None:
        """
        Create all collections if not exists. Loads `self._collections` for use.

        :param skip_existing: skip collection init if collection already exists in db
        """
        for collection_definition in self.topology:
            _ = self.create_collection(collection_definition, skip_existing)

    def create_collection(
            self,
            definition: types.CollectionDefinition,
            skip_existing: bool = True,
    ) -> Collection:
        """
        Create new entity and bind it to this database.
        As side effect collection is added to `self._collections`.

        :param definition: CollectionDefinition to create
        :param skip_existing: skip collection init if collection already exists in db
        """
        if definition.name in self._collections:
            # already exists, skip initialization
            return self._collections[definition.name]
        mongo_collection, is_new = self._create_mongo_collection(definition)
        new_collection = Collection(
            mongo_collection=mongo_collection,
            engine=self.engine,
        )
        if is_new or not skip_existing:
            self._create_indices(definition, mongo_collection)
            _ = self._insert_defaults(definition.default_docs, new_collection)

        self._collections[new_collection.name] = new_collection
        return new_collection

    def ping(self) -> bool:
        """
        Perform a ping command on database.

        :return: true if PING return OK, false otherwise.
        """
        return self.engine.ping(database=self.name)

    def _create_mongo_collection(
            self,
            definition: types.CollectionDefinition,
    ) -> Tuple[pymongo.collection.Collection, bool]:
        """
        Create a new collection in this database if not exists, return existing otherwise.

        :param definition:
        :return: tuple(collection, is created as new ?)
        """
        if definition.name in self._database.list_collection_names(nameOnly=True):
            self.logger.debug(f"{self.name} - fetched existing collection {definition.name!r}")
            return self._database[definition.name], False
        self.logger.warning(f"{self.name} - missing collection {definition.name!r}")

        try:
            new_collection = self._database.create_collection(definition.name)
            self.logger.info(f"{self.name} - created collection {definition.name!r}")
        except pymongo.errors.CollectionInvalid:
            self.logger.info(f"{self.name} - (probable race condition) skipped collection create {definition.name!r}")
            time.sleep(0.5)
            new_collection = self._database[definition.name]
        return new_collection, True

    def _create_indices(
            self,
            definition: types.CollectionDefinition,
            mongo_collection: pymongo.collection.Collection,
            silent: bool = True,
    ) -> None:
        """
        Create indices if not exists on collection.

        :param mongo_collection: collection to create indices on
        :param definition: collection structure
        :param silent: suppress errors?
        :raises PyMongoError: propagate mongo error when cannot create index; only if not silent
        """
        definition.fill_index_names()
        for index in definition.indices:
            if index.name not in mongo_collection.index_information():
                # in place check -> prevents a few conflicts, not all of them
                self._create_index(mongo_collection, index, silent)

    def _create_index(
            self,
            mongo_collection: pymongo.collection.Collection,
            index: types.Index,
            silent: bool = True,
    ) -> None:
        """
        Create index over collection.

        :param mongo_collection: collection to create indices on
        :param index: describes index
        :param silent: suppress errors?
        :raises PyMongoError: propagate mongo error when cannot create index; only if not silent
        """
        self.logger.debug(f"{mongo_collection.full_name} - creating index {index.name!r}")
        try:
            mongo_collection.create_index(
                keys=index.field_for_mongo(),
                name=index.name,
                unique=index.unique,
            )
            self.logger.info(f"{mongo_collection.full_name} - created index {index.name!r}")
        except PyMongoError as e:
            self.logger.warning(f"{mongo_collection.full_name} - failed to create index {index.name!r} because '{e}'")
            if not silent:
                raise e from e
            self.logger.warning(traceback.format_stack())

    def _insert_defaults(
            self,
            docs: Iterable[types.Document],
            collection: Collection,
    ) -> int:
        """
        Insert default data from definition into collection.

        :param docs: default docs
        :param collection: new collection to insert defaults into
        :return: docs created
        """
        new_docs_cnt = 0
        coll_name = collection.mongo_collection.full_name
        for doc in docs:
            if not collection.find_one(doc.unique_key, {"_id": 1}):
                self.logger.debug(f"{coll_name} - inserting default {doc!r}")
                _ = collection.insert_one(doc.data)
                new_docs_cnt += 1
                self.logger.debug(f"{coll_name} - inserted new default {doc!r}")
                self.logger.info(f"{coll_name} - inserted new default {doc.unique_key!r}")
        return new_docs_cnt
