__all__ = ['SSTable']

import os
import sys
import mmap
import time
import struct
import decimal
decimal.getcontext().prec = 4

from .index import Index
from .offset import Offset

class SSTable(object):
    def __init__(self, table, t=None):
        self.table = table

        # sstable path
        data_path = self.table.db.store.data_path
        db_name = self.table.db.db_name
        table_name = self.table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        
        if not t: t = '%.4f' % time.time()
        self.t = t
        
        filename = 'commitlog-%s.sstable' % t
        path = os.path.join(dirpath, filename)
        self.path = path

        # offset and offset path
        filename = 'offset-%s.offset' % t
        path = os.path.join(dirpath, filename)
        self.offset = Offset(self, path)

        # index and index path
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
        self.offset.f = open(self.offset.path, 'wb')
        self.index.f = open(self.index.path, 'wb')
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.index.f.close()
        self.offset.f.close()
        self.f.close()
        return False

    # def __getitem__(self, key):
    #     value = self.get(key)
    #     return value

    def __add__(self, other):
        # FIXME:
        pass

    def _add_row(self, row):
        # sstable
        row_blob = SSTable._get_row_packed(self.table, row)
        sstable_pos = self.f.tell()
        self.f.write(row_blob)

        # offset
        sstable_pos_blob = Offset._get_sstable_pos_packed(
            self, sstable_pos)
        self.offset.f.write(sstable_pos_blob)

        # index
        key_blob = Index._get_key_packed(self, row, sstable_pos)
        self.index.f.write(key_blob)

    @classmethod
    def _get_row_packed(cls, table, row):
        row_blob_items = []

        for c, t in table.schema:
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

        for c, t in table.schema:
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
        self.offset.open()
        self.index.open()

    def close(self):
        self.index.close()
        self.offset.close()
        self.mm.close()
        self.f.close()

    def get(self, key):
        sstable_pos = self.index._get_sstable_pos(key)
        row = SSTable._get_row_unpacked(self.table, self.mm, sstable_pos)
        return row

    def get_lt(self, key):
        sstable_pos = self.index._get_lt_sstable_pos(key)
        row = SSTable._get_row_unpacked(self.table, self.mm, sstable_pos)
        return row, sstable_pos

    def get_le(self, key):
        sstable_pos = self.index._get_le_sstable_pos(key)
        row = SSTable._get_row_unpacked(self.table, self.mm, sstable_pos)
        return row, sstable_pos

    def get_gt(self, key):
        # print 'get_gt:', key
        sstable_pos = self.index._get_gt_sstable_pos(key)
        row = SSTable._get_row_unpacked(self.table, self.mm, sstable_pos)
        return row, sstable_pos

    def get_ge(self, key):
        sstable_pos = self.index._get_ge_sstable_pos(key)
        row = SSTable._get_row_unpacked(self.table, self.mm, sstable_pos)
        return row, sstable_pos
