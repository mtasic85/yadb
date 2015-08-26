__all__ = ['Database']

import os
import sys

from .table import Table

class Database(object):
    def __init__(self, store, db_name):
        self.store = store
        self.db_name = db_name
        self.tables = []

    def close(self):
        for t in self.tables:
            t.close()

    @classmethod
    def create(cls, store, db_name):
        # create database dir
        dirpath = os.path.join(store.data_path, db_name)

        try:
            os.makedirs(dirpath)
        except OSError as e:
            pass

        # database
        db = Database(store, db_name)
        return db

    def table(self, table_name, **type_fields):
        # table
        table = Table.create(self, table_name, type_fields)
        self.tables.append(table)
        return table
