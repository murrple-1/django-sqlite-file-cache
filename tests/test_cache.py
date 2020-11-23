import unittest
import os
import sqlite3

from django_sqlite_file_cache import SQLiteFileCache


class TestCache(unittest.TestCase):
    location = 'test.db'

    def tearDown(self):
        super().setUp()

        os.unlink(self.location)

    def test_get(self):
        cache = SQLiteFileCache(self.location, {})

        self.assertIsNone(cache.get('my_key'))

        cache.set('my_key', 'value')

        self.assertEqual(cache.get('my_key'), 'value')

    def test_get_without_file(self):
        cache = SQLiteFileCache(self.location, {})

        os.unlink(self.location)

        self.assertIsNone(cache.get('my_key'))

    def test_add(self):
        cache = SQLiteFileCache(self.location, {})

        self.assertTrue(cache.add('my_key', 'value'))

    def test_add_after_set(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set('my_key', 'value1')

        self.assertFalse(cache.add('my_key', 'value2'))

    def test_set_after_set(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set('my_key', 'value1')

        cache.set('my_key', 'value2')

        self.assertEqual(cache.get('my_key'), 'value2')

    def test_touch(self):
        cache = SQLiteFileCache(self.location, {})

        self.assertFalse(cache.touch('my_key'))

        cache.set('my_key', 'value')

        self.assertTrue(cache.touch('my_key'))

    def test_delete(self):
        cache = SQLiteFileCache(self.location, {})

        self.assertFalse(cache.delete('my_key'))

        cache.set('my_key', 'value')

        self.assertTrue(cache.delete('my_key'))

    def test_clear(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set('my_key1', 'value1')
        cache.set('my_key2', 'value2')

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertGreater(count, 0)

        cache.clear()

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertEqual(count, 0)


if __name__ == '__main__':
    unittest.main()
