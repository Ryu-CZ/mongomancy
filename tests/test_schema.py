import logging
import typing as t
import unittest
from collections import OrderedDict

import mongomock

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

    def create_all(self):
        db = mongomancy.Database(name=self.DB_NAME, logger=self.logger, engine=self.engine)
        db.add_collection(self.collection_definition)
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
        with self.assertRaises(KeyError):
            _ = db[self.NOT_COLLECTION_NAME]
        with self.assertRaises(AttributeError):
            _ = getattr(db, self.NOT_COLLECTION_NAME)

    def test_create_all(self):
        self.create_all()

    def test_double_create_all(self):
        if self.engine:
            self.engine.drop_database(self.DB_NAME)
        self.create_all()
        self.create_all()


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
