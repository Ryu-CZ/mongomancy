import unittest

import mongomock

from src import mongomancy


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.engine = mongomancy.Engine("localhost", 27017, mongo_client_cls=mongomock.MongoClient)

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

    def test_get_database(self):
        db_ = self.engine.get_database("test")
        self.assertIsNotNone(db_)


if __name__ == "__main__":
    unittest.main()
