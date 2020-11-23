import unittest
import os

from django_sqlite_file_cache import SQLiteFileCache


class TestCache(unittest.TestCase):
    location = 'test.db'

    def tearDown(self):
        super().setUp()

        os.unlink(TestCache.location)

    def test_init(self):
        cache = SQLiteFileCache(TestCache.location, {})

    def test_set(self):
        cache = SQLiteFileCache(TestCache.location, {})
        cache.set('my_key', 'value')

    def test_get(self):
        cache = SQLiteFileCache(TestCache.location, {})

        self.assertIsNone(cache.get('my_key'))

    def test_get_after_set(self):
        cache = SQLiteFileCache(TestCache.location, {})

        cache.set('my_key', 'value')

        self.assertEqual(cache.get('my_key'), 'value')

    def test_get_without_file(self):
        cache = SQLiteFileCache(TestCache.location, {})

        os.unlink(TestCache.location)

        self.assertIsNone(cache.get('my_key'))


if __name__ == '__main__':
    unittest.main()
