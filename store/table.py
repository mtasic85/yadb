__all__ = ['Table']

import os
import sys
from collections import OrderedDict

from .schema import Schema
from .memtable import MemTable
from .sstable import SSTable
from .query import Query

class Table(object):
    MEMTABLE_LIMIT_N_ITEMS = 1000

    def __init__(self, db, table_name):
        self.store = db.store
        self.db = db
        self.table_name = table_name

        # load schema
        schema = Schema(self)
        schema.load()
        self.schema = schema

        # memtable
        self.memtable = MemTable()

        # sstables
        self.sstables = []

    def __getattr__(self, attr):
        pass

    @classmethod
    def create(cls, db, table_name, _type_fields):
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

        # create table dir inside of database dir
        dirpath = os.path.join(db.store.data_path, db.db_name, table_name)
        
        try:
            os.makedirs(dirpath)
        except OSError as e:
            pass

        # create schema
        schema = Schema.create(db, table_name, type_fields)

        table = Table(db, table_name)
        return table

    @property
    def query(self):
        q = Query(self.db.store)
        return q

    def commit_if_required(self):
        # if len(self.memtable) >= self.MEMTABLE_LIMIT_N_ITEMS:
        #     self.commit()
        pass

    def commit(self):
        cl = CommitLog(self)
        cl.save()

        self.memtable = MemTable()

    def insert(self, **row):
        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self.table, self.commit_insert, (), row))

    def commit_insert(self, **row):
        # compare against schema
        for k, v in row.items():
            if k not in self.schema.type_fields:
                raise Exception('filed %r is not defined in schema for table %r' % (k, self.table_name))

        # set default columns
        for k, v in self.schema.type_fields.items():
            if k == 'primary_key':
                continue

            if k not in row:
                row[k] = None

        # build key
        key = tuple(row[k] for k in self.schema.type_fields['primary_key'])

        # insert key
        self.memtable[key] = row

        # commit if required
        self.commit_if_required()

    def get(self, *args):
        key = tuple(args)
        return self.memtable[key]
