import os
import sys

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

    def create_table(self, table_name, **type_fields):
        table = Table.create(self, table_name, type_fields)
        return table