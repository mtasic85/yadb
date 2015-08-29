__all__ = ['Database']

import os
import sys

from .table import Table

class Database(object):
    def __init__(self, store, db_name):
        self.store = store
        self.db_name = db_name
        self.opened = False
        self.tables = []

        # database dir
        dirpath = self.get_database_path()

        if not os.path.exists(dirpath):
            try:
                os.makedirs(dirpath)
            except OSError as e:
                raise Exception('could not create database %r' % db_name)

    def __enter__(self):
        # open self if not
        if not self.is_opened():
            self.open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_opened():
            self.close()

        return False

    def get_database_path(self):
        return os.path.join(self.store.get_store_path(), self.db_name)

    def is_opened(self):
        return self.opened

    def open(self):
        # database dir
        dirpath = self.get_database_path()

        for table_name in os.listdir(dirpath):
            print 'table_name:', table_name

        self.opened = True

    def close(self):
        for t in self.tables:
            if t.is_opened():
                t.close()

        self.opened = False

    def table(self, table_name, **type_fields):
        # open self if not
        if not self.is_opened():
            self.open()

        # table
        table = Table.create(self, table_name, type_fields)
        self.tables.append(table)
        return table
