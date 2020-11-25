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

        row = self._conn.execute(
            '''SELECT value, expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,)).fetchone()

        if row is not None:
            value, expires_at = row
            if self._is_expired(expires_at):
                self._delete(key)
                return default
            else:
                return pickle.loads(zlib.decompress(value))
        else:
            return default

    def get_many(self, keys, version=None):
        if len(keys) == 0:
            return {}

        new_keys = []
        for key in keys:
            new_key = self.make_key(key, version=version)
            self.validate_key(new_key)
            new_keys.append(new_key)

        key_values = {}

        expired_keys = []

        for row in self._conn.execute('''SELECT key, value, expires_at FROM cache_entries WHERE key IN ({})'''.format(','.join(['?'] * len(new_keys))), new_keys):
            key, value, expires_at = row
            if self._is_expired(expires_at):
                expired_keys.append(key)
            else:
                key_values[key] = pickle.loads(zlib.decompress(value))

        if len(expired_keys) > 0:
            with self._conn:
                self._conn.execute('''DELETE FROM cache_entries WHERE key IN ({})'''.format(','.join(['?'] * len(expired_keys))), expired_keys)

        return key_values

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        self._createfile()

        self._cull()

        expiry = self.get_backend_timeout(timeout)

        pickled_value = zlib.compress(
            pickle.dumps(value, self.pickle_protocol))

        with self._conn:
            self._conn.execute(
                '''INSERT INTO cache_entries (key, value, expires_at) VALUES (?, ?, ?)''', (key, pickled_value, expiry))

    def set_many(self, data, timeout=DEFAULT_TIMEOUT, version=None):
        rekeyed_data = {}
        for key, value in data.items():
            key = self.make_key(key, version=version)
            self.validate_key(key)

            rekeyed_data[key] = value

        self._createfile()

        self._cull()

        expiry = self.get_backend_timeout(timeout)

        with self._conn:
            for key, value in rekeyed_data.items():
                pickled_value = zlib.compress(pickle.dumps(value, self.pickle_protocol))

                self._conn.execute('''INSERT INTO cache_entries (key, value, expires_at) VALUES (?, ?, ?)''', (key, pickled_value, expiry))

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        row = self._conn.execute(
            '''SELECT expires_at FROM cache_entries WHERE key = ?''', (key,)).fetchone()

        if row is not None:
            expires_at, = row
            if self._is_expired(expires_at):
                self._delete(key)
                return False
            else:
                expiry = self.get_backend_timeout(timeout)
                with self._conn:
                    cur = self._conn.execute(
                        '''UPDATE cache_entries SET expires_at = ? WHERE key = ?''', (expiry, key,))
                    return cur.rowcount > 0
        else:
            return False

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        row = self._conn.execute(
            '''SELECT expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,)).fetchone()

        if row is not None:
            expires_at, = row
            self._delete(key)

            if self._is_expired(expires_at):
                return False
            else:
                return True
        else:
            return False

    def delete_many(self, keys, version=None):
        rekeyed_keys = []
        for key in keys:
            key = self.make_key(key, version=version)
            self.validate_key(key)

            rekeyed_keys.append(key)

        with self._conn:
            self._conn.execute('''DELETE FROM cache_entries WHERE key IN ({})'''.format(','.join(['?'] * len(rekeyed_keys))), rekeyed_keys)

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        row = self._conn.execute(
            '''SELECT expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,)).fetchone()

        if row is not None:
            expires_at, = row
            if self._is_expired(expires_at):
                self._delete(key)
                return False
            else:
                return True
        else:
            return False

    def clear(self):
        with self._conn:
            self._conn.execute('''DELETE FROM cache_entries''')

    def _createfile(self):
        self._conn.execute('''
            CREATE TABLE IF NOT EXISTS cache_entries
            (
                key TEXT NOT NULL,
                value BLOB NOT NULL,
                expires_at REAL NOT NULL,
                PRIMARY KEY (key) ON CONFLICT REPLACE
            )
        ''')

    def _cull(self):
        count = self._conn.execute(
            '''SELECT COUNT(key) FROM cache_entries''').fetchone()[0]
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

            with self._conn:
                for key in keys:
                    self._conn.execute(
                        '''DELETE FROM cache_entries WHERE key = ?''', (key,))

    def _is_expired(self, expires_at):
        return expires_at < time.time()

    def _delete(self, key):
        with self._conn:
            self._conn.execute(
                '''DELETE FROM cache_entries WHERE key = ?''', (key,))
