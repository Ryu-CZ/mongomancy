import dataclasses
import typing as t
import unittest

from unittest import mock
import pymongo.client_session
import pymongo.errors
import mongomock
import mongomock.mongo_client
from src import mongomancy


def new_mock_engine() -> mongomancy.Engine[mongomock.MongoClient]:
    return mongomancy.Engine(
        "localhost",
        27017,
        mongo_client_cls=mongomock.MongoClient,
        auth_source="admin",
        auth_mechanism="SCRAM-SHA-256",
        user="test_user",
        password="test_password",
    )


class TestConnections(unittest.TestCase):
    engine: mongomancy.Engine

    def setUp(self):
        self.reconnected_flag = False
        self.engine = new_mock_engine()

    def tearDown(self):
        if self.engine is not None:
            self.engine.dispose()

    def set_flag(self, engine):
        _ = engine
        self.reconnected_flag = True

    def test_hook(self):
        self.engine.register_hook(self.set_flag)
        self.engine.reconnect()
        self.assertTrue(getattr(self, "reconnected_flag"))

    def test_create(self):
        self.assertIsNotNone(self.engine)
        self.assertIsNotNone(self.engine.client)

    def test_str(self):
        self.assertIsInstance(str(self.engine), str)

    def test_repr(self):
        self.assertIsInstance(repr(self.engine), str)

    def test_ping(self):
        is_ok = self.engine.ping()
        self.assertTrue(is_ok, "mongo server cannot be reached on ping")

    def test_disposed_ping(self):
        self.engine.dispose()
        is_ok = self.engine.ping()
        self.assertTrue(is_ok, "mongo server cannot be reached after dispose")

    def test_dispose(self):
        self.engine.dispose()
        self.assertTrue(self.engine.disposed)
        # test multiple dispose calls
        self.engine.dispose()
        self.engine.dispose()
        self.assertTrue(self.engine.disposed)

    def test_reconnect(self):
        self.engine.reconnect()
        self.assertFalse(self.engine.disposed)

    def test_connect_error(self):
        for err_cls in (IOError, pymongo.errors.PyMongoError):
            with mock.patch("mongomock.mongo_client.MongoClient.close") as moc_close:
                moc_close.side_effect = err_cls("Mocked Error")
                try:
                    self.engine.dispose()
                except err_cls as err_inst:
                    self.assertIsNone(err_inst)
                self.assertTrue(self.engine.disposed)

    def test_reconnect_close_error(self):
        for err_cls in (IOError, pymongo.errors.PyMongoError):
            with mock.patch("mongomock.mongo_client.MongoClient.close") as moc_close:
                moc_close.side_effect = err_cls("Mocked Error")
                try:
                    self.engine.reconnect()
                except err_cls as err_inst:
                    self.assertIsNone(err_inst)
                self.assertFalse(self.engine.disposed)

    def test_reconnect_new_client_error(self):
        original_client = self.engine.client
        for err_cls in (IOError, pymongo.errors.PyMongoError):
            mocked_client_cls = mock.Mock()
            mocked_client_cls.side_effect = err_cls("<Mocked Error>")
            self.engine.mongo_client_cls = mocked_client_cls
            with self.assertRaises(err_cls):
                self.engine.reconnect()
        self.assertEqual(original_client, self.engine.client)

    # not supported by mongomock
    # def test_session(self):
    #     with self.engine.start_session() as session_:
    #         self.assertIsInstance(session_, pymongo.client_session.ClientSession)

    def test_get_database(self):
        db_ = self.engine.get_database("test")
        self.assertIsNotNone(db_)


@dataclasses.dataclass
class TestCollection(mongomancy.types.CollectionContainer):
    dialect_entity: pymongo.collection.Collection


class TestQueries(unittest.TestCase):
    collection: TestCollection
    engine: mongomancy.Engine[mongomock.MongoClient]

    DB_NAME: t.ClassVar[str] = "engine_unit_tests"
    COLLECTION_NAME: t.ClassVar[str] = "dummy"
    ROWS: t.ClassVar[list[dict[str, t.Any]]] = [
        {"_id": 1, "NAME": "Pratchett"},
        {"_id": 2, "NAME": "Gaiman"},
        {"_id": 42, "NAME": "Kanturek"},
    ]
    UNKNOWN_DOC: t.ClassVar[dict[str, t.Any]] = {"_id": 404, "NAME": "Marquis"}
    CHANGES: t.ClassVar[dict[str, t.Any]] = {"$set": {"updated": True}}
    PIPELINE = [{"$group": {"_id": "", "sum_ids": {"$sum": "$_id"}}}, {"$project": {"_id": 0, "total_ids": "$sum_ids"}}]

    def setUp(self):
        self.engine = new_mock_engine()
        self.collection = TestCollection(
            dialect_entity=self.engine.get_database(self.DB_NAME).get_collection(self.COLLECTION_NAME)
        )

    def tearDown(self):
        if self.collection:
            self.collection.dialect_entity.drop()
        if self.engine is not None:
            self.engine.dispose()

    def test_empty(self):
        self.collection.dialect_entity.drop()
        any_doc_ = self.engine.find_one(self.collection, {})
        self.assertIsNone(any_doc_)

    def test_find_one_in_empty(self):
        doc_ = self.engine.find_one(self.collection, self.UNKNOWN_DOC)
        self.assertIsNone(doc_)

    def test_find_one_in_empty_disposed(self):
        self.engine.dispose()
        doc_ = self.engine.find_one(self.collection, self.UNKNOWN_DOC)
        self.assertIsNone(doc_)

    def test_delete_one_from_empty(self):
        result = self.engine.delete_one(self.collection, self.UNKNOWN_DOC)
        self.assertEqual(result.deleted_count, 0)

    def test_insert_one_into_empty(self):
        self.collection.dialect_entity.drop()
        self.engine.insert_one(self.collection, self.ROWS[0])
        stored_doc = self.engine.find_one(self.collection, self.ROWS[0])
        self.assertEqual(stored_doc["_id"], self.ROWS[0]["_id"])
        self.assertEqual(stored_doc["NAME"], self.ROWS[0]["NAME"])

    def update_one_in_empty(self):
        result = self.engine.update_one(self.collection, self.UNKNOWN_DOC, self.CHANGES)
        self.assertEqual(result.matched_count, 0)

    def test_insert_duplicate(self):
        if not self.engine.find_one(self.collection, self.ROWS[0]):
            self.engine.insert_one(self.collection, self.ROWS[0])
        with self.assertRaises(pymongo.errors.DuplicateKeyError):
            self.engine.insert_one(self.collection, self.ROWS[0])

    def test_delete_existing(self):
        if not self.engine.find_one(self.collection, self.ROWS[0]):
            self.engine.insert_one(self.collection, self.ROWS[0])
        result = self.engine.delete_one(self.collection, self.ROWS[0])
        self.assertEqual(result.deleted_count, 1)

    def test_update_one_existing(self):
        if not self.engine.find_one(self.collection, self.ROWS[0]):
            self.engine.insert_one(self.collection, self.ROWS[0])
        result = self.engine.update_one(self.collection, self.ROWS[0], self.CHANGES)
        self.assertEqual(result.matched_count, 1)
        doc_ = self.engine.find_one(self.collection, self.ROWS[0])
        self.assertIn("updated", doc_)
        self.assertTrue(doc_.get("updated"))

    def test_insert_many(self):
        self.collection.dialect_entity.drop()
        result = self.engine.insert_many(self.collection, self.ROWS)
        self.assertIsNotNone(result)
        self.assertEqual(len(result.inserted_ids), len(self.ROWS))

    def test_delete_many(self):
        self.collection.dialect_entity.drop()
        insert_result = self.engine.insert_many(self.collection, self.ROWS)
        delete_result = self.engine.delete_many(self.collection, {})
        self.assertIsNotNone(delete_result)
        self.assertEqual(delete_result.deleted_count, len(insert_result.inserted_ids))
        self.assertEqual(delete_result.deleted_count, len(self.ROWS))

    def test_update_many(self):
        self.collection.dialect_entity.drop()
        insert_result = self.engine.insert_many(self.collection, self.ROWS)
        update_result = self.engine.update_many(self.collection, {}, self.CHANGES)
        self.assertEqual(update_result.matched_count, len(insert_result.inserted_ids))
        self.assertEqual(update_result.matched_count, len(self.ROWS))

    def test_find_many(self):
        self.collection.dialect_entity.drop()
        insert_result = self.engine.insert_many(self.collection, self.ROWS)
        found = list(self.engine.find(self.collection, {}))
        self.assertEqual(len(found), len(insert_result.inserted_ids))
        self.assertEqual(len(found), len(self.ROWS))

    def test_aggregate(self):
        self.collection.dialect_entity.drop()
        _ = self.engine.insert_many(self.collection, self.ROWS)
        summed = list(self.engine.aggregate(self.collection, self.PIPELINE))
        self.assertEqual(1, len(summed))
        self.assertEqual(summed[0]["total_ids"], sum(doc["_id"] for doc in self.ROWS))

    def test_find_one_and_update(self):
        if not self.engine.find_one(self.collection, self.ROWS[0]):
            self.engine.insert_one(self.collection, self.ROWS[0])
        result = self.engine.find_one_and_update(self.collection, self.ROWS[0], self.CHANGES)
        self.assertIsNotNone(result)
        doc_ = self.engine.find_one(self.collection, self.ROWS[0])
        self.assertIn("updated", doc_)
        self.assertTrue(doc_.get("updated"))


if __name__ == "__main__":
    unittest.main()
