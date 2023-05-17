import dataclasses
import logging
import typing as t
import unittest

import mongomock
import pymongo.client_session
import pymongo.errors

from src import mongomancy


def new_mock_engine() -> mongomancy.Engine[mongomock.MongoClient]:
    return mongomancy.Engine("localhost", 27017, mongo_client_cls=mongomock.MongoClient)


class TestDatabase(unittest.TestCase):
    engine: mongomancy.Engine
    logger: logging.Logger
    DB_NAME: t.ClassVar[str] = "engine_unit_tests"
    COLLECTION_NAME: t.ClassVar[str] = "dummy"

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
