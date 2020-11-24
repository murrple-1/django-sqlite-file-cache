# django-sqlite-file-cache
A Django cache backend which has a similar API to FileBasedCache, but reduces the number of inodes used.

[![Build Status](https://travis-ci.com/murrple-1/django-sqlite-file-cache.svg?branch=main)](https://travis-ci.com/murrple-1/django-sqlite-file-cache) [![Coverage Status](https://coveralls.io/repos/github/murrple-1/django-sqlite-file-cache/badge.svg?branch=main)](https://coveralls.io/github/murrple-1/django-sqlite-file-cache?branch=main) [![PyPI version](https://badge.fury.io/py/django-sqlite-file-cache.svg)](https://badge.fury.io/py/django-sqlite-file-cache)

Supports both Python `>=v2.7` and `>=3.4`.

# Installation

`pip install django-sqlite-file-cache`

# Usage

## Add to your Django CACHES

```python
CACHES = {
    'default': {
        'BACKEND': 'django_sqlite_file_cache.SQLiteFileCache',
        'LOCATION': '/var/tmp/django_cache/cache.db',
    }
}
```

Ensure `LOCATION` path is readable/writable by the web server's user

See https://docs.djangoproject.com/en/3.1/topics/cache/ for more options
