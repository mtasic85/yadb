__all__ = ['Database']

import os
import sys
from collections import OrderedDict

from .table import Table

class Database(object):
    def __init__(self, store, db_name):
        self.store = store
        self.db_name = db_name

    @classmethod
    def create(cls, store, db_name):
        # create database dir
        dirpath = os.path.join(store.data_path, db_name)

        try:
            os.makedirs(dirpath)
        except OSError as e:
            pass

        db = Database(store, db_name)
        return db

    def create_table(self, table_name, **_type_fields):
        # sort type_fields
        type_fields = OrderedDict(
            (c, t)
            for c, t in sorted(_type_fields.items(), key=lambda n: n[0])
            if c != 'primary_key'
        )

        # add primary_key at the end of dict
        if 'primary_key' in _type_fields:
            v = _type_fields['primary_key']
            type_fields['primary_key'] = v
        
        table = Table.create(self, table_name, type_fields)
        return table
