import abc
import datetime as dt
from dataclasses import dataclass, field
from typing import (
    OrderedDict as OrderedDictType,
    List, Tuple, Sequence, Union, Optional, Mapping, Dict, Any, Iterable, Callable
)

import pymongo.results
import pymongo.database
from bson import ObjectId

__all__ = (
    "Bson",
    "BsonDict",
    "BsonList",
    "Index",
    "Document",
    "CollectionDefinition",
    "Executor",
)

Bson = Union[None, int, float, bool, str, ObjectId, dt.datetime, Sequence["Bson"], Mapping[str, "Bson"]]
BsonDict = Mapping[str, Bson]
BsonList = Sequence[Bson]


@dataclass(init=False)
class Index:
    """Collection Index data container"""
    fields: OrderedDictType[str, Union[str, int]]
    name: Optional[str]
    unique: Optional[bool]

    __slots__ = (
        "fields",
        "name",
        "unique",
    )

    def __init__(
            self,
            fields: OrderedDictType[str, Union[str, int]],
            name: Optional[str] = None,
            unique: Optional[bool] = False,
    ) -> None:
        self.fields = fields
        self.name = name
        self.unique = unique

    def field_for_mongo(self) -> List[Tuple[str, Union[str, int]]]:
        """Format out fields as order of tuples"""
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
    Collection Definition of name and indexed field
    """
    name: str
    indices: Sequence[Index] = field(default_factory=tuple, )
    default_docs: Sequence[Document] = field(default_factory=tuple, )

    def fill_index_names(self):
        """
        Do through indices and fill missing `index.name`s. Affects internal state of `self.indices` !
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
    def find_one(
            self,
            collection: pymongo.collection.Collection,
            where: Optional[BsonDict],
            *args,
            **kwargs,
    ) -> Optional[Dict[str, Any]]:
        ...

    @abc.abstractmethod
    def find(
            self,
            collection: pymongo.collection.Collection,
            where: Optional[BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.cursor.Cursor:
        ...

    @abc.abstractmethod
    def find_one_and_update(
            self,
            collection: pymongo.collection.Collection,
            where: BsonDict,
            changes: BsonDict | BsonList,
            *args,
            **kwargs,
    ) -> Optional[BsonDict]:
        ...

    @abc.abstractmethod
    def update_one(
            self,
            collection: pymongo.collection.Collection,
            where: Optional[BsonDict],
            changes: BsonDict | BsonList,
            *args,
            **kwargs,
    ) -> pymongo.results.UpdateResult:
        ...

    @abc.abstractmethod
    def update_many(
            self,
            collection: pymongo.collection.Collection,
            where: Optional[BsonDict],
            changes: BsonDict | BsonList,
            *args,
            **kwargs,
    ) -> pymongo.results.UpdateResult:
        ...

    @abc.abstractmethod
    def insert_one(
            self,
            collection: pymongo.collection.Collection,
            document: Optional[BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.InsertOneResult:
        ...

    @abc.abstractmethod
    def insert_many(
            self,
            collection: pymongo.collection.Collection,
            documents: Iterable[BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.InsertManyResult:
        ...

    @abc.abstractmethod
    def delete_one(
            self,
            collection: pymongo.collection.Collection,
            where: Optional[BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.DeleteResult:
        ...

    @abc.abstractmethod
    def delete_many(
            self,
            collection: pymongo.collection.Collection,
            where: Optional[BsonDict],
            *args,
            **kwargs,
    ) -> pymongo.results.DeleteResult:
        ...

    @abc.abstractmethod
    def register_hook(self, reconnect_hook: Callable[["Executor"], None]) -> None:
        ...

    @abc.abstractmethod
    def reconnect(self) -> None:
        ...

    @abc.abstractmethod
    def ping(self, database: Optional[str] = None) -> bool:
        ...

    @abc.abstractmethod
    def get_database(self, name: str) -> pymongo.database.Database:
        ...
