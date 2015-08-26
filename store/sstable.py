__all__ = ['SSTable']

import os
import sys
import mmap
import time
import struct
import decimal
decimal.getcontext().prec = 4

from index import Index

class SSTable(object):
    def __init__(self, table, t=None):
        self.table = table

        # sstable path
        data_path = self.table.db.store.data_path
        db_name = self.table.db.db_name
        table_name = self.table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        
        # t
        if not t:
            t = '%.4f' % time.time()

        self.t = t
        
        filename = 'commitlog-%s.sstable' % t
        path = os.path.join(dirpath, filename)
        self.path = path

        # index path
        filename = 'index-%s.index' % t
        path = os.path.join(dirpath, filename)
        self.index = Index(self, path)

        self.mm = None
        self.f = None

    def __repr__(self):
         return '<%s db: %r, table: %r, t: %r>' % (
            self.__class__.__name__,
            self.table.db.db_name,
            self.table.table_name,
            self.t,
        )

    def __enter__(self):
        self.f = open(self.path, 'wb')
        self.index.f = open(self.index.path, 'wb')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.index.f.close()
        self.f.close()
        return False

    def __getitem__(self, key):
        value = self.get(key)
        return value

    def __add__(self, other):
        # FIXME:
        pass

    def _add_row(self, row):
        row_blob = SSTable._get_row_packed(self.table, row)
        pos = self.f.tell()
        self.f.write(row_blob)

        key_blob = Index._get_key_packed(self.table, row, pos)
        self.index.f.write(key_blob)

    @classmethod
    def _get_row_packed(cls, table, row):
        row_blob_items = []

        for c, t in table.schema.type_fields.items():
            if c == 'primary_key':
                continue

            v = row.get(c, None)
            b = t._get_column_packed(v)
            row_blob_items.append(b)

        _row_blob = b''.join(row_blob_items)
        row_blob = struct.pack(b'!Q', len(_row_blob)) + _row_blob
        return row_blob

    @classmethod
    def _get_row_unpacked(cls, table, mm, pos):
        row_blob_len, = struct.unpack_from('!Q', mm, pos)
        row = {}
        p = pos + 8

        for c, t in table.schema.type_fields.items():
            if c == 'primary_key':
                continue

            v, p = t._get_column_unpacked(mm, p)
            row[c] = v

        return row

    @classmethod
    def create(cls, table, rows):
        # save
        sst = SSTable(table)

        with sst:
            for row in rows:
                sst._add_row(row)

        return sst

    def open(self):
        self.f = open(self.path, 'r+b')
        self.mm = mmap.mmap(self.f.fileno(), 0)

        self.index.open()

    def close(self):
        self.index.close()

        self.mm.close()
        self.f.close()

    def get(self, key):
        sstable_pos = self.index._get_sstable_pos(key)
        row = SSTable._get_row_unpacked(self.table, self.mm, sstable_pos)
        return row
