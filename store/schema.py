__all__ = ['Schema']

import os
import sys
import yaml
from collections import OrderedDict

class Schema(object):
    def __init__(self, table):
        self.table = table
        self.type_fields = None

    @classmethod
    def create(cls, db, table_name, type_fields):
        # save schema
        dirpath = os.path.join(db.store.data_path, db.db_name, table_name)
        filename = 'schema.yaml'
        path = os.path.join(dirpath, filename)
        _type_fields = dict(type_fields)
        
        with open(path, 'wb') as f:
            s = yaml.dump(_type_fields)
            f.write(s)

    def load(self):
        # load schema
        data_path = self.table.db.store.data_path
        db_name = self.table.db.db_name
        table_name = self.table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        filename = 'schema.yaml'
        path = os.path.join(dirpath, filename)
        
        with open(path, 'rb') as f:
            _type_fields = yaml.load(f)

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

        self.type_fields = type_fields
