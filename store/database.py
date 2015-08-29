__all__ = ['Database']

import os
import sys
from collections import OrderedDict

from .table import Table
from .column import Column

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

    def table(self, table_name, **_type_fields):
        # open self if not
        if not self.is_opened():
            self.open()

        # type_fields
        type_fields = OrderedDict()

        # sort type_fields
        _items = sorted(_type_fields.items(), key=lambda n: n[0])

        for column_name, column_type in _items:
            if column_name == 'primary_key':
                continue

            if column_type == 'bool':
                column = Column(column_name, column_type, 1)
            elif column_type == 'int':
                column = Column(column_name, column_type, 8)
            elif column_type == 'float':
                column = Column(column_name, column_type, 8)
            elif column_type.startswith('str'):
                if '[' in column_type:
                    s = column_type.index('[') + 1
                    e = column_type.index(']')
                    size = int(column_type[s:e])
                else:
                    size = None

                column = Column(column_name, column_type, size)
            else:
                raise Exception('unsupported column type')

            type_fields[column_name] = column

        # add primary_key at the end of dict
        if 'primary_key' not in _type_fields:
            raise Exception('primary_key is missing, but it is required')

        column_names = _type_fields['primary_key']

        for column_name in column_names:
            column = type_fields[column_name]

            if column.type == 'str' and column.size is None:
                raise Exception(
                    'Primary key\'s column with type'
                    '"str" must have fixed size'
                )

        type_fields['primary_key'] = column_names

        # table
        table = Table(self, table_name, type_fields)
        self.tables.append(table)
        return table
