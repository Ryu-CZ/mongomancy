import logging
import typing as t
import unittest
from collections import OrderedDict
from unittest import mock
import functools
import mongomock
import pymongo.errors

from src import mongomancy


def new_mock_engine() -> mongomancy.Engine[mongomock.MongoClient]:
    return mongomancy.Engine("localhost", 27017, mongo_client_cls=mongomock.MongoClient)


class TestDatabase(unittest.TestCase):
    engine: mongomancy.Engine
    logger: logging.Logger
    DB_NAME: t.ClassVar[str] = "engine_unit_tests"

    def setUp(self):
        self.logger = logging.getLogger(self.DB_NAME)
        self.reconnected_flag = False
        self.engine = new_mock_engine()

    def tearDown(self):
        if self.engine is not None:
            self.engine.dispose()

    def new_database(self):
        return mongomancy.Database(name=self.DB_NAME, logger=self.logger, engine=self.engine)

    def test_create(self):
        database = self.new_database()
        self.assertIsNotNone(database)

    def test_ping(self):
        db = self.new_database()
        pinged = db.ping()
        self.assertTrue(pinged)

    def test_dropping(self):
        db = self.new_database()
        self.engine.drop_database(db.name)
        self.assertNotIn(db.name, self.engine.client.list_database_names())


class TestIndex(unittest.TestCase):
    FIELDS: OrderedDict[str, t.Union[str, int]] = OrderedDict(name=1)
    NAME: t.Optional[str] = "name_order"
    UNIQUE: t.Optional[bool] = True
    DEFAULT_UNIQUE: t.Optional[bool] = False

    def test_default_not_unique(self):
        new_index = mongomancy.Index(fields=self.FIELDS, name=self.NAME)
        self.assertEqual(self.DEFAULT_UNIQUE, new_index.unique)

    def test_values(self):
        new_index = mongomancy.Index(fields=self.FIELDS, name=self.NAME, unique=self.UNIQUE)
        self.assertEqual(new_index.fields, self.FIELDS)
        self.assertEqual(new_index.name, self.NAME)
        self.assertEqual(new_index.unique, self.UNIQUE)


class TestSchemaInit(unittest.TestCase):
    engine: mongomancy.Engine
    logger: logging.Logger
    index: mongomancy.Index
    anonymous_index: mongomancy.Index
    DB_NAME: t.ClassVar[str] = "schema_init_unit_tests"
    COLLECTION_NAME: t.ClassVar[str] = "dummy"
    NOT_COLLECTION_NAME: t.ClassVar[str] = "not_exiting_collection"

    def setUp(self):
        self.logger = logging.getLogger(self.DB_NAME)
        self.reconnected_flag = False
        self.engine = new_mock_engine()
        self.index = mongomancy.Index(
            OrderedDict(value=1, name=1), f"ix_{self.COLLECTION_NAME}_value_name", unique=False
        )
        self.anonymous_index = mongomancy.Index(OrderedDict(test=1, knight=1), unique=False)
        self.collection_definition = mongomancy.CollectionDefinition(
            name=self.COLLECTION_NAME, indices=[self.index, self.anonymous_index]
        )

    def tearDown(self):
        if self.engine is not None:
            self.engine.drop_database(self.DB_NAME)
            self.engine.dispose()

    def create_all(self, as_add=True) -> mongomancy.Database:
        if as_add:
            db = mongomancy.Database(name=self.DB_NAME, logger=self.logger, engine=self.engine)
            db.add_collection(self.collection_definition)
        else:
            db = mongomancy.Database(self.DB_NAME, self.logger, self.engine, self.collection_definition)
        db.create_all()
        self.assertIsInstance(str(db), str)
        self.assertIsInstance(repr(db), str)
        self.assertIn(self.collection_definition.name, db.list_collection_names())
        coll = db.get_collection(self.COLLECTION_NAME)
        self.assertIsNotNone(coll)
        self.assertEqual(coll.name, self.COLLECTION_NAME)
        self.assertEqual(db[self.COLLECTION_NAME], coll)
        self.assertEqual(getattr(db, self.COLLECTION_NAME), coll)
        self.assertEqual(coll.pymongo_collection, coll.dialect_entity)
        pymongo_collection = coll.dialect_entity
        coll.pymongo_collection = None
        self.assertIsNone(coll.pymongo_collection)
        coll.pymongo_collection = pymongo_collection
        self.assertEqual(coll.pymongo_collection, pymongo_collection)
        self.assertTrue(self.COLLECTION_NAME in db)
        self.assertFalse(self.NOT_COLLECTION_NAME in db)
        with self.assertRaises(KeyError):
            _ = db[self.NOT_COLLECTION_NAME]
        with self.assertRaises(AttributeError):
            _ = getattr(db, self.NOT_COLLECTION_NAME)
        return db

    def test_create_all(self):
        if self.engine:
            self.engine.drop_database(self.DB_NAME)
        _ = self.create_all()

    def test_create_all_quick(self):
        if self.engine:
            self.engine.drop_database(self.DB_NAME)
        _ = self.create_all(as_add=False)

    def test_double_create_all(self):
        if self.engine:
            self.engine.drop_database(self.DB_NAME)
        db = self.create_all()
        db.create_all()
        self.create_all()

    def test_re_create_all(self):
        if self.engine:
            self.engine.drop_database(self.DB_NAME)
        _ = self.create_all()
        _ = self.create_all()

    def test_create_collection_fail(self):
        if self.engine:
            self.engine.drop_database(self.DB_NAME)

        class RaisingDB(mongomancy.Database):
            def create_collection(self, definition: mongomancy.types.CollectionDefinition, skip_existing: bool = True):
                if definition.name == TestSchemaInit.COLLECTION_NAME:
                    raise IOError("<Mocked Error>")
                return super().create_collection(definition, skip_existing)

        db = RaisingDB(name=self.DB_NAME, logger=self.logger, engine=self.engine)
        db.add_collection(self.collection_definition)
        print(db.topology)
        with self.assertRaises(IOError):
            db.create_all(skip_existing=False)


class SchemaSetup(unittest.TestCase):
    engine: mongomancy.Engine
    logger: logging.Logger
    collection: mongomancy.Collection
    db: mongomancy.Database

    DB_NAME: t.ClassVar[str] = "schema_queries_unit_tests"
    COLLECTION_NAME: t.ClassVar[str] = "game"
    DOCS = [
        {"_id": 0, "name": "tails_of_iron", "genre": "adventure"},
        {"_id": 1, "name": "witcher_3", "genre": "adventure"},
        {"_id": 2, "name": "warcraft_3", "genre": "strategy"},
    ]

    def setUp(self):
        self.logger = logging.getLogger(self.DB_NAME)
        self.reconnected_flag = False
        self.engine = new_mock_engine()
        self.engine.drop_database(self.DB_NAME)
        self.index = mongomancy.Index(
            OrderedDict(value=1, name=1), f"ix_{self.COLLECTION_NAME}_value_name", unique=False
        )
        self.db = mongomancy.Database(name=self.DB_NAME, logger=self.logger, engine=self.engine)
        self.db.add_collection(mongomancy.CollectionDefinition(name=self.COLLECTION_NAME, indices=[self.index]))
        self.db.create_all()
        self.collection = self.db[self.COLLECTION_NAME]

    def tearDown(self):
        if self.engine is not None:
            self.engine.drop_database(self.DB_NAME)
            self.engine.dispose()


class IndexSetup(SchemaSetup):
    DB_NAME: t.ClassVar[str] = "schema_index_tests"

    class TestDatabase(mongomancy.Database):
        def create_index(
            self, pymongo_collection: pymongo.collection.Collection, index: mongomancy.Index, silent: bool
        ):
            self._create_index(pymongo_collection, index, silent=silent)

    db: TestDatabase

    def setUp(self):
        self.logger = logging.getLogger(self.DB_NAME)
        self.reconnected_flag = False
        self.engine = new_mock_engine()
        self.engine.drop_database(self.DB_NAME)
        self.db = self.TestDatabase(name=self.DB_NAME, logger=self.logger, engine=self.engine)
        self.index = mongomancy.Index(
            OrderedDict(value=1, name=1), f"ix_{self.COLLECTION_NAME}_value_name", unique=False
        )
        self.db.add_collection(mongomancy.CollectionDefinition(name=self.COLLECTION_NAME, indices=[self.index]))
        self.db.create_all()
        self.collection = self.db[self.COLLECTION_NAME]

    def test_create_index_raises(self):
        coll_ = self.db.get_collection(self.COLLECTION_NAME)
        index_ = mongomancy.Index(OrderedDict(value=1, name=1), f"ix_{self.COLLECTION_NAME}_value_name", unique=False)
        err_cls = pymongo.errors.PyMongoError
        with mock.patch("mongomock.collection.Collection.create_index") as moc_close:
            moc_close.side_effect = err_cls("<Mocked Error>")
            with self.assertRaises(err_cls):
                self.db.create_index(coll_.pymongo_collection, index_, silent=False)

    def test_create_index_silent(self):
        coll_ = self.db.get_collection(self.COLLECTION_NAME)
        index_ = mongomancy.Index(OrderedDict(value=1, name=1), f"ix_{self.COLLECTION_NAME}_value_name", unique=False)
        err_cls = pymongo.errors.PyMongoError
        with mock.patch("mongomock.collection.Collection.create_index") as moc_close:
            moc_close.side_effect = err_cls("<Mocked Error>")
            try:
                self.db.create_index(coll_.pymongo_collection, index_, silent=True)
                ignored = True
            except err_cls:
                ignored = False
            self.assertTrue(ignored)


class CollectionSetup(SchemaSetup):
    DB_NAME: t.ClassVar[str] = "schema_collection_tests"
    new_collection_definition: mongomancy.CollectionDefinition

    def setUp(self):
        self.logger = logging.getLogger(self.DB_NAME)
        self.reconnected_flag = False
        self.engine = new_mock_engine()
        self.engine.drop_database(self.DB_NAME)
        self.db = mongomancy.Database(name=self.DB_NAME, logger=self.logger, engine=self.engine)
        self.index = mongomancy.Index(
            OrderedDict(value=1, name=1), f"ix_{self.COLLECTION_NAME}_value_name", unique=False
        )
        self.new_collection_definition = mongomancy.CollectionDefinition(
            name=self.COLLECTION_NAME, indices=[self.index]
        )

    def test_create_collection_raises(self):
        with mock.patch("mongomock.database.Database.create_collection") as mocked_create:
            for err_cls in (pymongo.errors.CollectionInvalid, pymongo.errors.OperationFailure):
                mocked_create.side_effect = err_cls("<Mocked Error>")
                coll_ = self.db.create_collection(self.new_collection_definition)
                self.assertIsNotNone(coll_)


class TestSchemaInsert(SchemaSetup):
    def insert_docs(self):
        _ = self.collection.insert_many(self.DOCS)

    def test_one(self):
        write_result = self.collection.insert_one(self.DOCS[0])
        self.assertIsNotNone(write_result.inserted_id)

    def test_many(self):
        write_result = self.collection.insert_many(self.DOCS[1:])
        self.assertEqual(len(write_result.inserted_ids), len(self.DOCS[1:]))


class TestSchemaFind(TestSchemaInsert):
    def setUp(self):
        super().setUp()
        self.insert_docs()

    def test_one(self):
        doc = self.collection.find_one({"name": self.DOCS[0]["name"]})
        self.assertEqual(self.DOCS[0]["_id"], doc.get("_id"))

    def test_many(self):
        adventures = list(filter(lambda x: x.get("genre") == "adventure", self.DOCS))
        docs = list(self.collection.find({"genre": "adventure"}))
        self.assertEqual(len(docs), len(adventures))

    def test_reconnect_hooks(self):
        self.db.invalidate_cache_hook(source=self.engine)
        self.assertIsNotNone(self.collection.pymongo_collection)

    def test_one_retry(self):
        scenario = [
            pymongo.errors.AutoReconnect,
            pymongo.errors.WriteError("test error", code=10107),
            self.DOCS[0],
        ]
        with mock.patch("mongomock.collection.Collection.find_one") as moc_find:
            moc_find.side_effect = scenario
            doc = self.collection.find_one({"name": self.DOCS[0]["name"]})
            self.assertEqual(self.DOCS[0]["_id"], doc.get("_id"))

    def test_one_retry_raises_write(self):
        scenario = [
            pymongo.errors.WriteError("test error", code=-7),
            self.DOCS[0],
        ]
        with mock.patch("mongomock.collection.Collection.find_one") as moc_find:
            moc_find.side_effect = scenario
            with self.assertRaises(pymongo.errors.WriteError):
                _ = self.collection.find_one({"name": self.DOCS[0]["name"]})

    def test_one_retry_raises_unknown(self):
        scenario = [
            pymongo.errors.ConfigurationError("test ConfigurationError"),
            self.DOCS[0],
        ]
        with mock.patch("mongomock.collection.Collection.find_one") as moc_find:
            moc_find.side_effect = scenario
            with self.assertRaises(pymongo.errors.ConfigurationError):
                _ = self.collection.find_one({"name": self.DOCS[0]["name"]})

    def test_one_retry_raises_fail_all(self):
        with mock.patch("mongomock.collection.Collection.find_one") as moc_find:
            moc_find.side_effect = pymongo.errors.AutoReconnect
            with self.assertRaises(pymongo.errors.AutoReconnect):
                _ = self.collection.find_one({"name": self.DOCS[0]["name"]})


class TestSchemaUpdate(TestSchemaInsert):
    def setUp(self):
        super().setUp()
        self.insert_docs()

    def test_find_one_and_update(self):
        doc_before = self.collection.find_one_and_update(
            {"_id": self.DOCS[0]["_id"]}, {"$set": {"found_updated": True}}
        )
        self.assertNotIn("found_updated", doc_before)
        doc_updated = self.collection.find_one({"_id": self.DOCS[0]["_id"]})
        self.assertTrue(doc_updated.get("found_updated"))

    def test_one(self):
        changes = self.collection.update_one({"_id": self.DOCS[1]["_id"]}, {"$set": {"one_updated": True}})
        self.assertEqual(changes.modified_count, 1)
        doc_updated = self.collection.find_one({"_id": self.DOCS[1]["_id"]})
        self.assertTrue(doc_updated.get("one_updated"))

    def test_many(self):
        changes = self.collection.update_many({}, {"$set": {"many_updated": True}})
        self.assertEqual(changes.modified_count, len(self.DOCS))
        docs_updated = list(self.collection.find({"many_updated": True}))
        self.assertEqual(len(docs_updated), len(self.DOCS))


class TestSchemaRemove(TestSchemaInsert):
    def setUp(self):
        super().setUp()
        self.insert_docs()

    def test_one(self):
        changes = self.collection.delete_one({"_id": self.DOCS[1]["_id"]})
        self.assertEqual(changes.deleted_count, 1)

    def test_many(self):
        adventures = list(filter(lambda x: x.get("genre") == "adventure", self.DOCS))
        changes = self.collection.delete_many({"genre": "adventure"})
        self.assertEqual(changes.deleted_count, len(adventures))
