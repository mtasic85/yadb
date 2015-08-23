__all__ = ['SSTable']

import os
import sys
import yaml

import time
import decimal
decimal.getcontext().prec = 6

class SSTable(object):
    def __init__(self, table):
        self.table = table
        self.sorted_rows = None

    def set_rows(self, rows):
        # self.rows = rows # dict: {k: v, k: v, ..}
        sorted_rows = rows.items() # list: [(k, v), (k, v), ...]
        sorted_rows.sort(key=lambda n: n[0])
        sorted_rows = [[list(k), v] for k, v in sorted_rows]
        self.sorted_rows = sorted_rows

    def load(self, filename):
        pass

    def save(self):
        # save
        data_path = self.table.db.store.data_path
        db_name = self.table.db.db_name
        table_name = self.table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        filename = 'commitlog-%s.yaml' % str(decimal.Decimal(time.time()))
        path = os.path.join(dirpath, filename)
        
        with open(path, 'wb') as f:
            s = yaml.dump(self.sorted_rows)
            f.write(s)

    def get(self, key):
        # FIXME:
        value = None
        return value
