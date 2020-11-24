import os
import pickle
import time
import zlib
import sqlite3

from django.core.cache.backends.base import DEFAULT_TIMEOUT, BaseCache


class SQLiteFileCache(BaseCache):
    pickle_protocol = pickle.HIGHEST_PROTOCOL

    def __init__(self, location, params):
        super().__init__(params)
        self._conn = sqlite3.connect(location)
        self._createfile()

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        if self.has_key(key, version):
            return False

        self.set(key, value, timeout, version)
        return True

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        try:
            cur = self._conn.execute(
                '''SELECT value, expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,))
            row = cur.fetchone()

            if row is not None:
                if row[1] < time.time():
                    self._conn.execute(
                        '''DELETE FROM cache_entries WHERE key = ?''', (key,))
                    self._conn.commit()
                    return default
                else:
                    return pickle.loads(zlib.decompress(row[0]))
            else:
                return default
        except sqlite3.OperationalError:
            return default

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        self._createfile()

        self._cull()

        cur = self._conn.execute(
            '''SELECT expires_at FROM cache_entries WHERE key = ?''', (key,))
        row = cur.fetchone()

        expiry = self.get_backend_timeout(timeout)

        pickled_value = zlib.compress(pickle.dumps(value, self.pickle_protocol))

        if row is not None:
            self._conn.execute(
                '''UPDATE cache_entries SET value = ?, expires_at = ? WHERE key = ?''', (pickled_value, expiry, key,))
        else:
            self._conn.execute(
                '''INSERT INTO cache_entries (key, value, expires_at) VALUES (?, ?, ?)''', (key, pickled_value, expiry))

        del pickled_value

        self._conn.commit()

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        try:
            cur = self._conn.execute(
                '''SELECT value, expires_at FROM cache_entries WHERE key = ?''', (key,))
            row = cur.fetchone()

            if row is not None:
                if row[1] < time.time():
                    self._conn.execute(
                        '''DELETE FROM cache_entries WHERE key = ?''', (key,))
                    self._conn.commit()
                    return False
                else:
                    expiry = self.get_backend_timeout(timeout)
                    self._conn.execute(
                        '''UPDATE cache_entries SET expires_at = ? WHERE key = ?''', (expiry, key,))
                    self._conn.commit()
                    return True
            else:
                return False
        except sqlite3.OperationalError:
            return False

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        try:
            cur = self._conn.execute(
                '''SELECT expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,))
            row = cur.fetchone()

            if row is not None:
                self._conn.execute(
                    '''DELETE FROM cache_entries WHERE key = ?''', (key,))
                self._conn.commit()

                if row[0] < time.time():
                    return False
                else:
                    return True
            else:
                return False
        except sqlite3.OperationalError:
            return False

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        try:
            cur = self._conn.execute(
                '''SELECT expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,))
            row = cur.fetchone()

            if row is not None:
                if row[0] < time.time():
                    self._conn.execute(
                        '''DELETE FROM cache_entries WHERE key = ?''', (key,))
                    self._conn.commit()
                    return False
                else:
                    return True
            else:
                return False
        except sqlite3.OperationalError:
            return False

    def clear(self):
        try:
            self._conn.execute('''DELETE FROM cache_entries''')
            self._conn.commit()
        except sqlite3.OperationalError:
            pass

    def _createfile(self):
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS cache_entries
            (
                key TEXT NOT NULL PRIMARY KEY,
                value BLOB NOT NULL,
                expires_at REAL NOT NULL
            )
        ''')

    def _cull(self):
        cur = self._conn.execute('''SELECT COUNT(key) FROM cache_entries''')
        count = cur.fetchone()[0]
        if count < self._max_entries:
            return
        elif self._cull_frequency == 0:
            self.clear()
            return
        else:
            limit = int(count / self._cull_frequency)
            cur = self._conn.execute(
                '''SELECT key FROM cache_entries ORDER BY RANDOM() LIMIT ?''', (limit,))
            keys = map(lambda row: row[0], cur.fetchall())

            for key in keys:
                self._conn.execute(
                    '''DELETE FROM cache_entries WHERE key = ?''', (key,))

            self._conn.commit()
