# lshash/storage.py
# Copyright 2012 Kay Zhu (a.k.a He Zhu) and contributors (see CONTRIBUTORS.txt)
#
# This module is part of lshash and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import unicode_literals

import json

try:
    import redis
except ImportError:
    redis = None

try:
    from elasticsearch import Elasticsearch
except ImportError:
    Elasticsearch = None


__all__ = ['storage']


def storage(storage_config, index):
    """ Given the configuration for storage and the index, return the
    configured storage instance.
    """
    if 'dict' in storage_config:
        return InMemoryStorage(storage_config['dict'])
    elif 'redis' in storage_config:
        return RedisStorage(storage_config['redis'], index)
    elif 'es' in storage_config:
        return ElasticSearchStorage(storage_config['es'])
    else:
        raise ValueError("Only in-memory dictionary and Redis are supported.")


class BaseStorage(object):
    def keys(self):
        """ Returns a list of binary hashes that are used as dict keys. """
        raise NotImplementedError

    def append_val(self, key, val):
        """ Append `val` to the list stored at `key`.

        If the key is not yet present in storage, create a list with `val` at
        `key`.
        """
        raise NotImplementedError

    def get_list(self, key):
        """ Returns a list stored in storage at `key`.

        This method should return a list of values stored at `key`. `[]` should
        be returned if the list is empty or if `key` is not present in storage.
        """
        raise NotImplementedError


class InMemoryStorage(BaseStorage):
    def __init__(self, h_index):
        self.name = 'dict'
        self.storage = dict()

    def keys(self):
        return self.storage.keys()

    def append_val(self, key, val):
        self.storage.setdefault(key, set()).update([val])

    def get_list(self, key):
        return list(self.storage.get(key, []))


class RedisStorage(BaseStorage):
    def __init__(self, config, h_index):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = redis.StrictRedis(**config)
        # a single db handles multiple hash tables, each one has prefix ``h[h_index].``
        self.h_index = 'h%.2i.' % int(h_index)

    def _list(self, key):
        return self.h_index + key

    def keys(self, pattern='*'):
        # return the keys BUT be agnostic with reference to the hash table
        return [k.decode('ascii').split('.')[1] for k in self.storage.keys(self.h_index + pattern)]

    def append_val(self, key, val):
        self.storage.sadd(self._list(key), json.dumps(val))

    def get_list(self, key):
        _list = list(self.storage.smembers(self._list(key)))  # list elements are plain strings here
        _list = [json.loads(el.decode('ascii')) for el in _list]  # transform strings into python tuples
        for el in _list:
            # if len(el) is 2, then el[1] is the extra value associated to the element
            if len(el) == 2 and type(el[0]) == list:
                el[0] = tuple(el[0])
        _list = [tuple(el) for el in _list]
        return _list


class ElasticSearchStorage(BaseStorage):
    def __init__(self, config):
        self.name = 'es'
        self.index = config['index']
        self.doc_type = config['doc_type']
        self.storage = Elasticsearch(config['connections'])

    def keys(self):
        ids = helpers.scan(self.storage,
             index=self.index,
             doc_type=self.doc_type,
             body={'fields': ['key'], "query": {"match_all": {}}})
        return ids

    def append_val(self, key, val):
        extra = val[-1]
        val = tuple([x for i, x in enumerate(val) if i+1 < len(val)])
        body = {
            'key': key,
            'val': val,
            'extra': extra
        }
        self.storage.index(self.index, self.doc_type, json.dumps(body))

    def get_list(self, key):
        res = self.storage.search(self.index, self.doc_type, {'query': {'match': {'key': key}}})
        _list = []
        for hit in res['hits']['hits']:
            val = hit['_source']['val']
            extra = hit['_source']['extra']
            if extra and type(val[0]) == list:
                val[0] = tuple(val[0])
            _list.append((val[0], extra))
        return _list
