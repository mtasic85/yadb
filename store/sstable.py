__all__ = ['SSTable']

import os
import sys
import mmap
import time
import struct

from .index import Index
from .offset import Offset

class SSTable(object):
    def __init__(self, table, t=None, rows=None):
        self.table = table
        if not t: t = '%.4f' % time.time()
        self.t = t
        self.opened = False

        # offset
        offset = Offset(self, t)
        self.offset = offset

        # indexes
        self.indexes = {}

        # index by primary key
        indexed_columns = (tuple(table.schema.primary_key),)

        # index each column in primary key
        # used for ranged queries
        indexed_columns += tuple((n,) for n in table.schema.primary_key)

        for n in indexed_columns:
            index = Index(self, t, n)
            self.indexes[n] = index

        self.f = None
        self.mm = None

        # rows
        if rows:
            self.w_open()
            self._add_rows(rows)
            self.w_close()

    def __repr__(self):
         return '<%s db: %r, table: %r, t: %r>' % (
            self.__class__.__name__,
            self.table.db.db_name,
            self.table.table_name,
            self.t,
        )

    def __enter__(self):
        '''
        Used only on data writing to file.
        '''
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        '''
        Used only on data writing to file.
        '''
        self.close()
        return False

    def __add__(self, other):
        # FIXME:
        pass

    def get_path(self):
        filename = 'sstable-%s.data' % self.t
        path = os.path.join(self.table.get_path(), filename)
        return path

    def is_opened(self):
        return self.opened

    def open(self):
        '''
        Used only on data reading from file.
        '''
        self.f = open(self.get_path(), 'r+b')
        self.mm = mmap.mmap(self.f.fileno(), 0)
        self.offset.open()
        
        for column_names, index in self.indexes.items():
            index.open()

        self.opened = True

    def close(self):
        '''
        Used only on data reading from file.
        '''
        for column_names, index in self.indexes.items():
            index.close()

        self.offset.close()
        self.mm.close()
        self.f.close()
        self.opened = False

    def w_open(self):
        '''
        Open file for writing.
        '''
        self.f = open(self.get_path(), 'wb')
        self.offset.w_open()
        
        for column_names, index in self.indexes.items():
            index.w_open()

    def w_close(self):
        '''
        Close file for writing.
        '''
        for column_names, index in self.indexes.items():
            index.w_close()

        self.offset.w_close()
        self.f.close()

    def _add_rows(self, rows):
        for row in rows:
            self._add_row(row)

    def _add_row(self, row):
        # sstable
        sstable_pos = self.f.tell()
        self._write_row(row)

        # offset
        self.offset._write_sstable_pos(sstable_pos)

        # index
        for column_names, index in self.indexes.items():
            index._write_key(row, sstable_pos)

    def _write_row(self, row):
        table = self.table
        row_blob_items = []

        for c, t in table.schema:
            v = row.get(c, None)
            b = t._get_column_packed(v)
            row_blob_items.append(b)

        _row_blob = b''.join(row_blob_items)
        _row_size = struct.pack(b'!Q', len(_row_blob))
        self.f.write(_row_size)
        self.f.write(_row_blob)

    def _read_row(self, pos):
        row_blob_len, = struct.unpack_from('!Q', self.mm, pos)
        row = {}
        p = pos + 8

        for c, t in self.table.schema:
            v, p = t._get_column_unpacked(self.mm, p)
            row[c] = v

        return row

    def get(self, key, columns=None):
        if columns: 
            columns = tuple(columns)
        else:
            columns = tuple(self.table.schema.primary_key)

        index = self.indexes[columns]
        offset_pos, sstable_pos = index.get_sstable_pos(key)
        row = self._read_row(sstable_pos)
        return row, offset_pos, sstable_pos

    def get_lt(self, key, columns=None):
        if columns: 
            columns = tuple(columns)
        else:
            columns = tuple(self.table.schema.primary_key)

        index = self.indexes[columns]
        offset_pos, sstable_pos = index.get_lt_sstable_pos(key)
        row = self._read_row(sstable_pos)
        return row, offset_pos, sstable_pos

    def get_le(self, key, columns=None):
        if columns: 
            columns = tuple(columns)
        else:
            columns = tuple(self.table.schema.primary_key)

        index = self.indexes[columns]
        offset_pos, sstable_pos = index.get_le_sstable_pos(key)
        row = self._read_row(sstable_pos)
        return row, offset_pos, sstable_pos

    def get_gt(self, key, columns=None):
        if columns: 
            columns = tuple(columns)
        else:
            columns = tuple(self.table.schema.primary_key)

        index = self.indexes[columns]
        offset_pos, sstable_pos = index.get_gt_sstable_pos(key)
        row = self._read_row(sstable_pos)
        return row, offset_pos, sstable_pos

    def get_ge(self, key, columns=None):
        if columns: 
            columns = tuple(columns)
        else:
            columns = tuple(self.table.schema.primary_key)

        index = self.indexes[columns]
        offset_pos, sstable_pos = index.get_ge_sstable_pos(key)
        row = self._read_row(sstable_pos)
        return row, offset_pos, sstable_pos
