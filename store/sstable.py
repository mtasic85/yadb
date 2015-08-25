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

    @classmethod
    def create(cls, rows):
        # rows: dict: {k: v, k: v, ..}
        sorted_rows = rows.items() # list: [(k, v), (k, v), ...]
        sorted_rows.sort(key=lambda n: n[0])
        sorted_rows = [[list(k), v] for k, v in sorted_rows]
        
        # save
        data_path = self.table.db.store.data_path
        db_name = self.table.db.db_name
        table_name = self.table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        filename = 'commitlog-%s.yaml' % str(decimal.Decimal(time.time()))
        path = os.path.join(dirpath, filename)
        
        # row struct
        row_struct = (
            b'!Q', # row len
        )

        # len for each column
        row_struct += tuple(n for n in self.table.schema.type_fields if n != 'primary_key')

        with open(path, 'wb') as f:
            # rows
            pass
