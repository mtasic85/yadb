__all__ = ['SSTable']

import os
import sys
import mmap
import time
import struct
import decimal
decimal.getcontext().prec = 6

class SSTable(object):
    def __init__(self, table):
        self.table = table
        self.mm = None

    def __add__(self, other):
        # FIXME:
        pass

    def open(self):
        # FIXME: open mmap'ed file
        pass

    @classmethod
    def _get_row_struct_packed(cls, table, row):
        row_blob_items = []

        for c, t in table.schema.type_fields.items():
            if c == 'primary_key':
                continue

            v = row.get(c, None)
            b = t._get_column_struct_packed(v)
            row_blob_items.append(b)

        _row_blob = b''.join(row_blob_items)
        row_blob = struct.pack(b'!Q', _row_blob) + _row_blob
        return row_blob

    @classmethod
    def create(cls, table, rows):
        # # rows: dict: {k: v, k: v, ..}
        # sorted_rows = rows.items() # list: [(k, v), (k, v), ...]
        # sorted_rows.sort(key=lambda n: n[0])
        # sorted_rows = [[list(k), v] for k, v in sorted_rows]
        
        # save
        data_path = table.db.store.data_path
        db_name = table.db.db_name
        table_name = table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        filename = 'commitlog-%s.yaml' % str(decimal.Decimal(time.time()))
        path = os.path.join(dirpath, filename)
        
        with open(path, 'wb') as f:
            for row in rows:
                # pack and write row
                row_blob = cls._get_row_struct_packed(table, row)
                f.write(row_blob)

        sst = SSTable(table)
        return sst
