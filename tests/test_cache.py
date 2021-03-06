import unittest
import os
import sqlite3

from django_sqlite_file_cache import SQLiteFileCache


class TestCache(unittest.TestCase):
    location = 'test.db'

    def tearDown(self):
        super().setUp()

        try:
            os.unlink(TestCache.location)
        except FileNotFoundError:
            pass

    def test_get(self):
        cache = SQLiteFileCache(self.location, {})

        self.assertIsNone(cache.get('my_key'))

        cache.set('my_key', 'value')

        self.assertEqual(cache.get('my_key'), 'value')

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

    def test_max_entries(self):
        cache = SQLiteFileCache(self.location, {
            'OPTIONS': {
                'MAX_ENTRIES': 10,
            },
        })

        self.assertEqual(cache._max_entries, 10)

        for i in range(cache._max_entries):
            cache.set(f'my_key{i}', 'value')

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertEqual(count, cache._max_entries)

        cache.set('a_key', 'value')

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertLess(count, cache._max_entries)

    def test_cull_frequency(self):
        cache = SQLiteFileCache(self.location, {
            'OPTIONS': {
                'MAX_ENTRIES': 10,
                'CULL_FREQUENCY': 0,
            },
        })

        self.assertEqual(cache._max_entries, 10)

        for i in range(cache._max_entries):
            cache.set(f'my_key{i}', 'value')

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertEqual(count, cache._max_entries)

        cache.set('a_key', 'value')

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_expiry(self):
        cache = SQLiteFileCache(self.location, {
            'TIMEOUT': 0,
        })

        cache.set('my_key', 'value')

        self.assertFalse(cache.has_key('my_key'))

        cache.set('my_key', 'value')

        self.assertIsNone(cache.get('my_key'))

        cache.set('my_key', 'value')

        self.assertFalse(cache.touch('my_key'))

        cache.set('my_key', 'value')

        self.assertFalse(cache.delete('my_key'))

    def test_missing_file(self):
        cache = SQLiteFileCache(self.location, {})

        os.unlink(self.location)

        self.assertFalse(cache.has_key('my_key'))

        self.assertIsNone(cache.get('my_key'))

        self.assertEqual(len(cache.get_many(['my_key1', 'my_key2'])), 0)

    def test_cache_in_memory(self):
        cache = SQLiteFileCache(':memory:', {})

        self.assertIsNone(cache.get('my_key'))

        cache.set('my_key', 'value')

        self.assertEqual(cache.get('my_key'), 'value')

    def test_set_get_many(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set_many({
            'my_key1': 'value',
            'my_key2': 4,
            'my_key3': True,
        })

        self.assertEqual(
            len(cache.get_many(['my_key1', 'my_key2', 'my_key3'])), 3)
        self.assertEqual(
            len(cache.get_many(['my_key1', 'my_key2', 'my_key3', 'extra_key'])), 3)

    def test_get_many_no_keys(self):
        cache = SQLiteFileCache(self.location, {})

        self.assertEqual(len(cache.get_many([])), 0)

    def test_delete_many(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set_many({
            'my_key1': 'value',
            'my_key2': 4,
            'my_key3': True,
        })

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertEqual(count, 3)

        cache.delete_many(['my_key1', 'my_key2'])

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        conn.close()
        self.assertEqual(count, 1)

    def test_get_many_expired(self):
        cache = SQLiteFileCache(self.location, {
            'TIMEOUT': 0,
        })

        cache.set_many({
            'my_key1': 'value',
            'my_key2': 4,
            'my_key3': True,
        })

        self.assertEqual(
            len(cache.get_many(['my_key1', 'my_key2', 'my_key3'])), 0)

    def test_sqlite_timeout(self):
        cache = SQLiteFileCache(self.location, {
            'OPTIONS': {
                'SQLITE_TIMEOUT': 1.0,
            },
        })

        cache.set('my_key', 'value')

        self.assertEqual(cache.get('my_key'), 'value')

    def test_noneexpiry(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set('my_key', 'value', None)

        self.assertEqual(cache.get('my_key'), 'value')

    def test_nonevalue(self):
        cache = SQLiteFileCache(self.location, {})

        cache.set('my_key', None)

        conn = sqlite3.connect(self.location)
        cur = conn.execute('''SELECT value FROM cache_entries WHERE key = ?''', (cache.make_key('my_key'),))
        value = cur.fetchone()[0]
        conn.close()
        self.assertIs(type(value), bytes)


if __name__ == '__main__':
    unittest.main()
