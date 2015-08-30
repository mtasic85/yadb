__all__ = ['Table']

import os
import sys
# from collections import OrderedDict

# from .column import Column
from .schema import Schema
from .memtable import MemTable
from .sstable import SSTable
from .query import Query
from .deferred import Deferred

class Table(object):
    MEMTABLE_LIMIT_N_ITEMS = 100

    def __init__(self, db, table_name, type_fields=None):
        self.store = db.store
        self.db = db
        self.table_name = table_name
        self.opened = False

        # schema
        self.schema = Schema(self, type_fields)

        # memtable
        self.memtable = MemTable(self)

        # sstables
        self.sstables = []
        table_path = self.get_path()

        for filename in os.listdir(table_path):
            if not filename.startswith('sstable-'):
                continue

            s = filename.index('sstable-') + len('sstable-')
            e = filename.index('.data')
            t = filename[s:e]

            # sstable
            sst = SSTable(self, t)
            sst.open() # FIXME: lazy open in SSTable only if required
            self.sstables.append(sst)

    def __getattr__(self, attr):
        c = getattr(self.schema, attr)
        return c

    def get_path(self):
        return os.path.join(self.db.get_path(), self.table_name)

    def is_opened(self):
        return self.opened

    def open(self):
        self.opened = True

    def close(self):
        for sst in self.sstables:
            if sst.is_opened():
                sst.close()

        self.opened = False

    def commit_if_required(self):
        if len(self.memtable) >= self.MEMTABLE_LIMIT_N_ITEMS:
            self.commit()

    def commit(self):
        # get sorted rows by primary_key
        columns = self.schema.primary_key
        rows = self.memtable.get_sorted_rows(columns)
        
        # create new sstable
        sst = SSTable(self, rows=rows)
        sst.open()
        self.sstables.append(sst)

        # clear memtable
        self.memtable = MemTable(self)

    @property
    def query(self):
        q = Query(self.db.store)
        return q

    def insert(self, **row):
        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self, Table._commit_insert, (self,), row))

    def _commit_insert(self, **row):
        # check if all columns exist in table's schema
        # compare against schema
        for k, v in row.items():
            if k not in self.schema:
                raise Exception(
                    'filed %r is not defined in schema for table %r' % (
                        k,
                        self.table_name,
                    )
                )

        # set default columns to None
        for k, v in self.schema:
            if k not in row:
                row[k] = None

        # build key
        key = tuple(row[k] for k in self.schema.primary_key)

        # insert key
        self.memtable.set(key, row)

        # commit if required
        self.commit_if_required()

    def get(self, *args):
        # key
        key = args

        # deferred
        d = Deferred()

        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self.table, Table._commit_get, (self, d, key), {}))

        return d
    
    def _commit_get(self, d, key):
        v, op, sp = self._get(key)
        d.set(v)

    def select(self, *args):
        # deferred, queue
        d = Deferred()
        q = Query(self.store, d)

        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self.table, Table._commit_select, (self, d, q), {}))

        return q

    def _commit_select(self, d, query):
        rows = []
        where_keys = []

        for where_clause in query.where_clauses:
            print 'where_clause:', where_clause

        for where_clause in query.where_clauses:
            print 'where_clause:', where_clause
            key = [None] * len(self.schema.primary_key)
            index = self.schema.primary_key.index(where_clause.left.name)
            key[index] = where_clause.right
            key = tuple(key)
            print 'key:', key

            if where_clause.op == '<':
                print 'lt:', self._get_lt(key)
            elif where_clause.op == '<=':
                print 'le:', self._get_le(key)
            elif where_clause.op == '>':
                print 'gt:', self._get_gt(key)
            elif where_clause.op == '>=':
                print 'ge:', self._get_ge(key)

        d.set(rows)

    def _get(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp

    def _get_lt(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_lt(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_lt(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp

    def _get_le(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_le(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_le(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp
    
    def _get_gt(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_gt(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_gt(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp
    
    def _get_ge(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_ge(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_ge(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp
