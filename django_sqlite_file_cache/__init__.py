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
        self._location = os.path.abspath(location)
        self._createfile()

    def add(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        if self.has_key(key, version):
            return False

        self.set(key, value, timeout, version)
        return True

    def get(self, key, default=None, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        conn = sqlite3.connect(self._location)
        try:
            cur = conn.execute('''SELECT value, expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,))
            row = cur.fetchone()

            if row is not None:
                if row[1] < time.time():
                    conn.execute('''DELETE FROM cache_entries WHERE key = ?''', (key,))
                    conn.commit()
                    return default
                else:
                    return pickle.loads(zlib.decompress(row[0]))
            else:
                return default
        except sqlite3.OperationalError:
            return default
        finally:
            conn.close()

    def set(self, key, value, timeout=DEFAULT_TIMEOUT, version=None):
        self._createfile()
        key = self.make_key(key, version=version)
        self.validate_key(key)

        self._cull()

        expiry = self.get_backend_timeout(timeout)

        value = zlib.compress(pickle.dumps(value, self.pickle_protocol))

        conn = sqlite3.connect(self._location)
        try:
            cur = conn.execute('''SELECT expires_at FROM cache_entries WHERE key = ?''', (key,))
            row = cur.fetchone()

            if row is not None:
                conn.execute('''UPDATE cache_entries SET value = ?, expires_at = ? WHERE key = ?''', (value, expiry, key,))
            else:
                conn.execute('''INSERT INTO cache_entries (key, value, expires_at) VALUES (?, ?, ?)''', (key, value, expiry))

            conn.commit()
        finally:
            conn.close()

    def touch(self, key, timeout=DEFAULT_TIMEOUT, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        conn = sqlite3.connect(self._location)
        try:
            cur = conn.execute('''SELECT value, expires_at FROM cache_entries WHERE key = ?''', (key,))
            row = cur.fetchone()

            if row is not None:
                if row[1] < time.time():
                    conn.execute('''DELETE FROM cache_entries WHERE key = ?''', (key,))
                    conn.commit()
                    return False
                else:
                    expiry = self.get_backend_timeout(timeout)
                    conn.execute('''UPDATE cache_entries SET expires_at = ? WHERE key = ?''', (expiry, key,))
                    conn.commit()
                    return True
            else:
                return False
        except sqlite3.OperationalError:
            return False
        finally:
            conn.close()

    def delete(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        conn = sqlite3.connect(self._location)
        try:
            cur = conn.execute('''SELECT expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,))
            row = cur.fetchone()

            if row is not None:
                conn.execute('''DELETE FROM cache_entries WHERE key = ?''', (key,))
                conn.commit()

                if row[0] < time.time():
                    return False
                else:
                    return True
            else:
                return False
        except sqlite3.OperationalError:
            return False
        finally:
            conn.close()

    def has_key(self, key, version=None):
        key = self.make_key(key, version=version)
        self.validate_key(key)

        conn = sqlite3.connect(self._location)
        try:
            cur = conn.execute('''SELECT expires_at FROM cache_entries WHERE key = ? LIMIT 1''', (key,))
            row = cur.fetchone()

            if row is not None:
                if row[0] < time.time():
                    conn.execute('''DELETE FROM cache_entries WHERE key = ?''', (key,))
                    conn.commit()
                    return False
                else:
                    return True
            else:
                return False
        except sqlite3.OperationalError:
            return False
        finally:
            conn.close()

    def clear(self):
        conn = sqlite3.connect(self._location)
        try:
            conn.execute('''DELETE FROM cache_entries''')
            conn.commit()
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()

    def _createfile(self):
        conn = sqlite3.connect(self._location)
        conn.execute('''
            CREATE TABLE IF NOT EXISTS cache_entries
            (
                key TEXT NOT NULL PRIMARY KEY,
                value BLOB NOT NULL,
                expires_at REAL NOT NULL
            )
        ''')
        conn.close()

    def _cull(self):
        conn = sqlite3.connect(self._location)

        try:
            cur = conn.execute('''SELECT COUNT(key) FROM cache_entries''')
            count = cur.fetchone()[0]
            if count < self._max_entries:
                return
            elif self._cull_frequency == 0:
                self.clear()
                return
            else:
                limit = int(count / self._cull_frequency)
                cur = conn.execute('''SELECT key from cache_entries ORDER BY RANDOM() LIMIT ?''', (limit,))
                keys = map(lambda row: row[0], cur.fetchall())

                for key in keys:
                    conn.execute('''DELETE FROM cache_entries WHERE key = ?''', (key,))

                conn.commit()
        finally:
            conn.close()
