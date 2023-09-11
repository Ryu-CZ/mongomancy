import atexit
import logging
import time
import traceback
from typing import (
    List,
    Sequence,
    Union,
    Set,
    TypeVar,
    Mapping,
    Any,
    ClassVar,
    Type,
    Optional,
    Callable,
    Iterable,
    Generic,
)

import pymongo
import pymongo.client_session
import pymongo.command_cursor
import pymongo.database
import pymongo.results
import pymongo.typings
from pymongo.errors import PyMongoError

from . import mongo_errors, types

__all__ = ("Engine",)

LoggerType = Union[logging.Logger, logging.LoggerAdapter]
_CommandReturn = TypeVar("_CommandReturn")

ExecutorLike = TypeVar("ExecutorLike", bound=types.Executor)
M = TypeVar("M")


class Engine(types.Executor, Generic[M]):
    """
    Client of Mongo Database storage. All DB communication should be handled through this class.
    """

    CONNECTION_ERRORS: ClassVar[Iterable[Type[pymongo.errors.PyMongoError]]] = (
        pymongo.errors.AutoReconnect,
        pymongo.errors.ConnectionFailure,
        IOError,
    )
    disposed: bool
    client: pymongo.MongoClient
    logger: LoggerType
    semaphore_tower: types.SemaphoreTower
    retry_codes: Set[int]
    write_retry: int
    write_retry_delay: Union[float, int]
    read_retry: int
    read_retry_delay: Union[float, int]
    _address: str
    _connection_params: Mapping[str, Any]
    reconnect_hooks: List[Callable[[ExecutorLike], None]]
    mongo_client_cls: Type[M]

    __slots__ = (
        "client",
        "logger",
        "retry_codes",
        "write_retry",
        "write_retry_delay",
        "read_retry",
        "read_retry_delay",
        "retry_codes",
        "_address",
        "_connection_params",
        "reconnect_hooks",
        "semaphore_tower",
        "disposed",
        "mongo_client_cls",
    )

    def __init__(
        self,
        host: str = "localhost",
        port: int = 27017,
        max_pool_size: int = 1,
        queue_timeout: int = 400,
        auth_source: Optional[str] = None,
        auth_mechanism: Optional[str] = None,
        user: Optional[str] = "kanturek",
        password: Optional[str] = "kajot",
        connect_timeout: Optional[int] = 15000,
        write_retry: int = 3,
        write_retry_delay: Union[float, int] = 1.3,
        read_retry: int = 2,
        read_retry_delay: Union[float, int] = 0.701,
        retry_codes: Optional[Sequence[int]] = None,
        logger: Optional[LoggerType] = None,
        mongo_client_cls: Optional[Type[M]] = pymongo.MongoClient,
        **kwargs,
    ) -> None:
        """
        Create connection engine for mongo(s) server.

        :param host: database server host
        :param port: database server port
        :param max_pool_size: To support concurrent MongoDB operations within one process increase this value to 2+.
        :param queue_timeout: [ms]Once the pool reaches its max size, additional threads are allowed to wait this time
        :param name: database to use for storage
        :param auth_source: database to auth user in
        :param auth_mechanism: mongo authentication mechanism
        :param user: mongo user to log in as
        :param password: password of used user
        :param connect_timeout: [ms]Controls how long the mongo_driver will wait during server monitoring when connecting a
            new socket to a server before concluding the server is unavailable. `0` or `None` means no timeout.
        :param kwargs: other named parameters for db
        :exception pymongo.errors.PyMongoError: if database connection could not be established
        """
        super(Engine, self).__init__()
        self.disposed = True
        self.mongo_client_cls = mongo_client_cls or pymongo.MongoClient
        self.reconnect_hooks = []
        self._address = f"{host}:{port}"
        self.logger = logger or logging.getLogger(type(self).__qualname__)
        self.semaphore_tower = types.SemaphoreTower(logger=self.logger)
        self.retry_codes = set(retry_codes or mongo_errors.DEFAULT_RETRY)
        # how many times to retry write on error 10107 (switched master)
        self.write_retry = max(0, write_retry or 0)
        # how much time to sleep between retry write on error 10107 (switched master)
        self.write_retry_delay = max(0, write_retry_delay or 0)
        self.read_retry = max(0, read_retry or 0)
        self.read_retry_delay = max(0, read_retry_delay or 0)
        authentication = {}
        if auth_mechanism:
            authentication["username"] = user
            authentication["password"] = password
            authentication["authMechanism"] = auth_mechanism
            if auth_source:
                authentication["authSource"] = auth_source
        self._connection_params = dict(
            host=host,
            port=port,
            maxPoolSize=max_pool_size,
            waitQueueTimeoutMS=queue_timeout,
            connectTimeoutMS=connect_timeout,
            **authentication,
            **kwargs,
        )
        with self.semaphore_tower:
            self.client = self._new_client()
            self.disposed = False
            atexit.register(self.dispose)

    def __repr__(self) -> str:
        return f"{type(self).__qualname__}(server={self.address!r})"

    def __str__(self) -> str:
        return f"{type(self).__name__}(server={self.address!r})"

    def _new_client(self) -> M:
        """Create new instance of Mongo Client from `self._connection_params`"""
        try:
            new_client = self.mongo_client_cls(**self._connection_params)
        except (PyMongoError, IOError, KeyError, ValueError) as e:
            self.logger.info(f"Cannot create mongo client because: {e}")
            self.logger.warning(traceback.format_stack())
            raise e from e
        return new_client

    @property
    def address(self):
        """Return address of mongo server"""
        return self._address

    def get_database(self, name: str) -> pymongo.database.Database:
        """
        Get pymongo database by NAME

        :param name: database NAME
        :return: pymongo database
        """
        return self.client.get_database(name)

    def drop_database(self, name: str, *args, **kwargs) -> None:
        return self.client.drop_database(name, *args, **kwargs)

    def register_hook(self, reconnect_hook: Callable[[ExecutorLike], None]) -> None:
        """
        Add hook to be called when client is reconnected.

        :param reconnect_hook: hook to be called when client is reconnected
        """
        self.reconnect_hooks.append(reconnect_hook)

    def dispose(self) -> None:
        """
        Cleanup client resources and disconnect from MongoDB. Thread locked and fork locked.
        """
        if self.disposed:
            return
        with self.semaphore_tower:
            if self.disposed:
                return
            return self._dispose()

    def _dispose(self) -> None:
        """
        Cleanup client resources and disconnect from MongoDB.
        """
        try:
            self.client.close()
        except (pymongo.errors.PyMongoError, *self.CONNECTION_ERRORS) as e:
            self.logger.debug(f"Cannot close mongo client because: {e}")
        self.disposed = True
        self.logger.debug(f"{self} - disconnect from MongoDB")

    def ping(self, database: Optional[str] = None) -> bool:
        """
        Is database server reachable?

        :param database: database NAME, otherwise just ping client by listing databases
        :return: Is database server reachable?
        """
        database = database or "admin"
        is_ok = False
        retry = max(1, self.read_retry)
        while not is_ok and retry > 0:
            retry -= 1
            if self.disposed:
                try:
                    self.reconnect()
                except PyMongoError:
                    is_ok = False
            try:
                is_ok = self.client[database].command("ping").get("ok")
            except self.CONNECTION_ERRORS as e:
                self.logger.info(f"failed to ping database={database!r} - e={e}")
                time.sleep(self.read_retry_delay)
                self.reconnect()
            except PyMongoError as e:
                self.logger.info(f"failed to ping database={database!r} - e={e}")
                self.logger.warning(traceback.format_stack())
        return bool(is_ok)

    def reconnect(self) -> None:
        """Close existing Mongo Client and create new one. Thread locked and fork locked."""
        with self.semaphore_tower:
            return self._reconnect()

    def _reconnect(self) -> None:
        """Close existing Mongo Client and create new one"""
        self.logger.debug(f"{type(self).__qualname__} - reconnecting client")
        try:
            self.client = self._new_client()
            self.disposed = False
            for hook in self.reconnect_hooks:
                _ = hook(self)
            self.logger.info(f"{type(self).__qualname__} - reconnected client")
        except (IOError, pymongo.errors.PyMongoError) as e:
            self.logger.error(f"{type(self).__qualname__} - cannot reconnect to mongo server because: {e}")
            raise e from e
        return

    def _retry_command(
        self,
        collection: types.CollectionContainer,
        command: types.METHOD,
        /,
        *args,
        command_name_: Optional[str] = None,
        **kwargs,
    ) -> _CommandReturn:
        """
        Execute command and retry if error is caused by switched master in remote DB cluster.

        :param collection: collection of method used; f.e.: `game` in `game.find_one`
        :param command: method to execute; f.e.: `game.find_one`
        :param args: arguments for command
        :param command_name_: use this as command NAME for logging
        :param kwargs: key word arguments for command
        :return: return same as command returns
        """
        result = None
        _error = False
        attempt = 0
        if self.disposed:
            time.sleep(self.write_retry_delay)
            self.reconnect()
        while attempt <= self.write_retry and _error is not None:
            attempt += 1
            command_fn = getattr(collection.dialect_entity, command)
            try:
                result = command_fn(*args, **kwargs)
                _error = None
            except self.CONNECTION_ERRORS as e:
                command_name_ = command_name_ or command or "<unknown_command>"
                self.logger.info(
                    f"fail {attempt}/{self.write_retry} - {command_name_} args={args}, kwargs={kwargs} e={e}"
                )
                _error = e
                time.sleep(self.write_retry_delay)
                self.reconnect()
            except pymongo.errors.WriteError as e:
                command_name_ = command_name_ or command or "<unknown_command>"
                if e.code not in self.retry_codes:
                    self.logger.error(f"failed to write - {command_name_} args={args}, kwargs={kwargs} e={e}")
                    raise e from e
                self.logger.info(
                    f"fail {attempt}/{self.write_retry} - {command_name_} args={args}, kwargs={kwargs} e={e}"
                )
                _error = e
                time.sleep(self.write_retry_delay)
                self.reconnect()
            if _error:
                collection = self.client[collection.dialect_entity.database.name][collection.dialect_entity.name]
        if _error:
            command_name_ = command_name_ or command or "<unknown_command>"
            self.logger.warning(f"fatal fail - {command_name_} args={args}, kwargs={kwargs} - after {attempt}x retry")
            raise _error from _error
        return result

    def start_session(
        self,
        causal_consistency: Optional[bool] = None,
        default_transaction_options: Optional[pymongo.client_session.TransactionOptions] = None,
        snapshot: Optional[bool] = False,
    ) -> pymongo.client_session.ClientSession:
        return self.client.start_session(causal_consistency, default_transaction_options, snapshot)

    def find_one(
        self, collection: types.CollectionContainer, where: Optional[types.BsonDict], *args, **kwargs
    ) -> Optional[types.BsonDict]:
        return self._retry_command(collection, "find_one", where, *args, **kwargs)

    def find(
        self, collection: types.CollectionContainer, where: Optional[types.BsonDict], *args, **kwargs
    ) -> pymongo.cursor.Cursor[types.BsonDict]:
        return self._retry_command(collection, "find", where, *args, **kwargs)

    def find_one_and_update(
        self,
        collection: types.CollectionContainer,
        where: types.BsonDict,
        changes: types.BsonDict | types.BsonList,
        *args,
        **kwargs,
    ) -> Optional[types.BsonDict]:
        return self._retry_command(collection, "find_one_and_update", where, changes, *args, **kwargs)

    def aggregate(
        self, collection: types.CollectionContainer, pipeline: types.BsonList, *args, **kwargs
    ) -> pymongo.command_cursor.CommandCursor:
        return self._retry_command(collection, "aggregate", pipeline, *args, **kwargs)

    def update_one(
        self,
        collection: types.CollectionContainer,
        where: Optional[types.BsonDict],
        changes: types.BsonDict | Sequence[Mapping[str, Any]],
        *args,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        return self._retry_command(collection, "update_one", where, changes, *args, **kwargs)

    def update_many(
        self,
        collection: types.CollectionContainer,
        where: Optional[types.BsonDict],
        changes: types.BsonDict | types.BsonList,
        *args,
        **kwargs,
    ) -> pymongo.results.UpdateResult:
        return self._retry_command(collection, "update_many", where, changes, *args, **kwargs)

    def insert_one(
        self, collection: types.CollectionContainer, document: types.BsonDict, *args, **kwargs
    ) -> pymongo.results.InsertOneResult:
        return self._retry_command(collection, "insert_one", document, *args, **kwargs)

    def insert_many(
        self, collection: types.CollectionContainer, documents: Iterable[types.BsonDict], *args, **kwargs
    ) -> pymongo.results.InsertManyResult:
        return self._retry_command(collection, "insert_many", documents, *args, **kwargs)

    def delete_one(
        self, collection: types.CollectionContainer, where: Optional[types.BsonDict], *args, **kwargs
    ) -> pymongo.results.DeleteResult:
        return self._retry_command(collection, "delete_one", where, *args, **kwargs)

    def delete_many(
        self, collection: types.CollectionContainer, where: Optional[types.BsonDict], *args, **kwargs
    ) -> pymongo.results.DeleteResult:
        return self._retry_command(collection, "delete_many", where, *args, **kwargs)
