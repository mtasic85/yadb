__all__ = ['Database']

import os
import sys

from .table import Table

class Database(object):
    def __init__(self, store, db_name):
        self.store = store
        self.db_name = db_name
        self.tables = []

        # database dir
        dirpath = os.path.join(store.data_path, db_name)

        if os.path.exists(dirpath):
            # list tables
            pass
        else:
            try:
                os.makedirs(dirpath)
            except OSError as e:
                raise Exception('could not create database %r' % db_name)

    def close(self):
        for t in self.tables:
            t.close()
    
    def table(self, table_name, **type_fields):
        # table
        table = Table.create(self, table_name, type_fields)
        self.tables.append(table)
        return table
