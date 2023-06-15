import inspect
import logging
import time
import traceback
from dataclasses import dataclass
from typing import List, Tuple, Union, TypeVar, Mapping, Any, Optional, Dict, Iterable, ClassVar

import pymongo
import pymongo.command_cursor
import pymongo.database
import pymongo.results
import pymongo.typings
from pymongo.errors import PyMongoError

from . import types

__all__ = ("Collection", "Database", "DocumentType")

LoggerType = Union[logging.Logger, logging.LoggerAdapter]
DocumentType = TypeVar("DocumentType", bound=Mapping[str, Any])


def define_lock_collection(name: str):
    return types.CollectionDefinition(
        name, default_docs=[types.Document(unique_key={"_id": "master"}, data={"_id": "master", "locked": False})]
    )


@dataclass(slots=True)
class Collection:
    """
    Abstraction of existing collection.
    Wraps `pymongo.collection.Collection` for easier reconnect.
    """

    dialect_entity: pymongo.collection.Collection
    engine: types.Executor

    @property
    def pymongo_collection(self) -> pymongo.collection.Collection:
        return self.dialect_entity

    @pymongo_collection.setter
    def pymongo_collection(self, value: pymongo.collection.Collection) -> None:
        self.dialect_entity = value

    @property
    def name(self) -> str:
        return self.pymongo_collection.name

    @property
    def full_name(self) -> str:
        return self.pymongo_collection.full_name

    def find_one(self, where: Optional[types.BsonDict], *args, **kwargs) -> Optional[DocumentType]:
        return self.engine.find_one(self, where, *args, **kwargs)

    def find(self, where: Optional[types.BsonDict], *args, **kwargs) -> pymongo.cursor.Cursor[types.BsonDict]:
        return self.engine.find(self, where, *args, **kwargs)

    def find_one_and_update(
        self, where: types.BsonDict, changes: types.BsonDict | types.BsonList, *args, **kwargs
    ) -> Optional[types.BsonDict]:
        return self.engine.find_one_and_update(self, where, changes, *args, **kwargs)

    def update_one(
        self, where: Optional[types.BsonDict], changes: types.BsonDict | types.BsonList, *args, **kwargs
    ) -> pymongo.results.UpdateResult:
        return self.engine.update_one(self, where, changes, *args, **kwargs)

    def update_many(
        self, where: Optional[types.BsonDict], changes: types.BsonDict | types.BsonList, *args, **kwargs
    ) -> pymongo.results.UpdateResult:
        return self.engine.update_many(self, where, changes, *args, **kwargs)

    def insert_one(self, document: types.BsonDict, *args, **kwargs) -> pymongo.results.InsertOneResult:
        return self.engine.insert_one(self, document, *args, **kwargs)

    def insert_many(self, documents: Iterable[types.BsonDict], *args, **kwargs) -> pymongo.results.InsertManyResult:
        return self.engine.insert_many(self, documents, *args, **kwargs)

    def delete_one(self, where: Optional[types.BsonDict], *args, **kwargs) -> pymongo.results.DeleteResult:
        return self.engine.delete_one(self, where, *args, **kwargs)

    def delete_many(self, where: Optional[types.BsonDict], *args, **kwargs) -> pymongo.results.DeleteResult:
        return self.engine.delete_many(self, where, *args, **kwargs)

    def aggregate(self, pipeline: Optional[types.List], *args, **kwargs) -> pymongo.command_cursor.CommandCursor:
        return self.engine.aggregate(self, pipeline, *args, **kwargs)


class Database:
    """
    Database abstraction with reconnect support

    Example of usage: ..
        engine = Engine("localhost", 27017)
        logger = logging.getLogger(__name__)
        db = Database(engine=engine, logger=logger)
        game = CollectionDefinition(NAME="game", indices=[Index(FIELDS={"game_id": 1})])
        player = CollectionDefinition(NAME="player", indices=[Index(FIELDS={"player_id": 1}, UNIQUE=True)])
        db.add_definition(game)
        db.add_definition(player)
        db.create_all()
        db["game"].find_one({"game_id": 1, "NAME": "game 1"})

    """

    engine: types.Executor
    topology: List[types.CollectionDefinition]
    _database: pymongo.database.Database
    _collections: Dict[str, Collection]
    semaphore_tower: types.SemaphoreTower
    logger: LoggerType
    LOCK_COLLECTION: ClassVar[str] = "mongomancy_lock"
    wait_step: float
    max_wait: float

    __slots__ = (
        "engine",
        "topology",
        "_database",
        "_collections",
        "semaphore_tower",
        "logger",
        "wait_step",
        "max_wait",
    )

    def __init__(
        self,
        name: str,
        logger: LoggerType,
        engine: types.Executor,
        *collections: types.CollectionDefinition,
        wait_step: float = 7,
        max_wait: float = 55,
    ) -> None:
        self.max_wait = max_wait
        self.wait_step = wait_step
        self._collections = {}
        self.topology = []
        self.logger = logger
        self.semaphore_tower = types.SemaphoreTower(logger=self.logger)
        self.engine = engine
        self._database = engine.get_database(name)
        for coll in collections:
            self.add_collection(coll)
        self.engine.register_hook(reconnect_hook=self.invalidate_cache_hook)

    def __getitem__(self, item: str) -> Collection:
        if item not in self._collections:
            raise KeyError(f"collection {item!r} not found")
        return self._collections[item]

    def __contains__(self, item) -> bool:
        return item in self._collections

    def __getattr__(self, item) -> Collection:
        if item in self._collections:
            return self._collections[item]
        return object.__getattr__(self, item)

    def __str__(self) -> str:
        return f"{type(self).__qualname__}(NAME={self.name!r})"

    @property
    def name(self) -> str:
        return self._database.name

    def invalidate_cache_hook(self, source: types.Executor) -> None:
        """
        Hook for handling reconnect event.
        Changes internal reference to pymongo database `self._database` together with
        contained `dialect_entity` of each `self._collections`.
        """
        self._database = source.get_database(self.name)
        self.logger.debug(f"{self}.invalidate_cache_hook - switched _database")
        for collection in self._collections.values():
            collection.dialect_entity = self._database[collection.name]
        self.logger.debug(f"{self}.invalidate_cache_hook - switch _collections bindings")

    def drop(self):
        self.engine.drop_database(self.name)

    def get_collection(self, name: str) -> Collection:
        """
        Get collection by NAME from existing `self._collections`.

        :param name: UNIQUE NAME of collection
        :return: instance of Collection or raise errors
        :raises KeyError: if there is no collection of that NAME
        """
        if name not in self._collections:
            raise KeyError(f"collection {name!r} not found")
        return self._collections[name]

    def add_collection(self, new_definitions: types.CollectionDefinition) -> None:
        """
        Add new collection definitions into `self.topology`.

        :param new_definitions: add the collection definition
        """
        self.topology.append(new_definitions)

    def _lock(self) -> bool:
        """
        Try to acquire lock or return false
        :returns: lock acquired
        """
        lock_collection = self.create_collection(define_lock_collection(self.LOCK_COLLECTION))
        doc = self.engine.find_one_and_update(
            lock_collection, where={"_id": "master", "locked": False}, changes={"$set": {"locked": True}}
        )
        if not doc:
            # failed to acquire
            return False
        # doc is return in "before update" state
        self.logger.debug(f"master lock state before lock update: {doc}")
        return not bool(doc.get("locked"))

    def _unlock(self):
        """
        Update master lock to unlock state.
        """
        lock_collection = Collection(dialect_entity=self._database[self.LOCK_COLLECTION], engine=self.engine)
        doc = self.engine.find_one_and_update(
            lock_collection, where={"_id": "master"}, changes={"$set": {"locked": False}}, upsert=True
        )
        self.logger.debug(f"master lock state before unlock update: {doc}")

    def create_all(self, skip_existing: bool = True) -> None:
        """
        Create all collections if not exists. Loads `self._collections` for use.

        :param skip_existing: skip collection init if collection already exists in db
        """
        with self.semaphore_tower:
            wait_time = 0
            try:
                while not self._lock():
                    self.logger.debug(f"create_all - process or thread is waiting for master lock {wait_time}sec")
                    time.sleep(self.wait_step)
                    wait_time += self.wait_step
                    if wait_time > self.max_wait:
                        self.logger.warning(f"create_all - wait timeout after {wait_time}sec and stops waiting")
                        self._unlock()
                        break
                for collection_definition in self.topology:
                    _ = self.create_collection(collection_definition, skip_existing)
            except (Exception, KeyError, IndexError, IOError) as e:
                self.logger.error(f"create_all failed on {e}")
                raise e from e
            finally:
                self._unlock()

    def create_collection(self, definition: types.CollectionDefinition, skip_existing: bool = True) -> Collection:
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
        new_collection = Collection(dialect_entity=mongo_collection, engine=self.engine)
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

    def list_collection_names(self) -> list[str]:
        if "nameOnly" in inspect.signature(self._database.list_collection_names).parameters:
            return self._database.list_collection_names(nameOnly=True)
        return self._database.list_collection_names()

    def _create_mongo_collection(
        self, definition: types.CollectionDefinition
    ) -> Tuple[pymongo.collection.Collection, bool]:
        """
        Create a new collection in this database if not exists, return existing otherwise.

        :param definition:
        :return: tuple(collection, is created as new ?)
        """
        if definition.name in self.list_collection_names():
            self.logger.debug(f"{self.name} - fetched existing collection {definition.name!r}")
            return self._database[definition.name], False
        self.logger.warning(f"{self.name} - missing collection {definition.name!r}")

        try:
            new_collection = self._database.create_collection(definition.name)
            self.logger.info(f"{self.name} - created collection {definition.name!r}")
        except (pymongo.errors.CollectionInvalid, pymongo.errors.OperationFailure) as e:
            self.logger.info(f"{self.name} - (probable race condition) skipped collection create {definition.name!r}")
            self.logger.debug(f"collection {definition.name!r} init failed on {e!r}")
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
        self, mongo_collection: pymongo.collection.Collection, index: types.Index, silent: bool = True
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
            mongo_collection.create_index(index.field_for_mongo(), name=index.name, unique=index.unique)
            self.logger.info(f"{mongo_collection.full_name} - created index {index.name!r}")
        except PyMongoError as e:
            self.logger.warning(f"{mongo_collection.full_name} - failed to create index {index.name!r} because '{e}'")
            if not silent:
                raise e from e
            self.logger.warning(traceback.format_stack())

    def _insert_defaults(self, docs: Iterable[types.Document], collection: Collection) -> int:
        """
        Insert default data from definition into collection.

        :param docs: default docs
        :param collection: new collection to insert defaults into
        :return: docs created
        """
        new_docs_cnt = 0
        coll_name = collection.full_name
        for doc in docs:
            if not collection.find_one(doc.unique_key, {"_id": 1}):
                self.logger.debug(f"{coll_name} - inserting default {doc!r}")
                _ = collection.insert_one(doc.data)
                new_docs_cnt += 1
                self.logger.debug(f"{coll_name} - inserted new default {doc!r}")
                self.logger.info(f"{coll_name} - inserted new default {doc.unique_key!r}")
        return new_docs_cnt
