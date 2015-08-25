__all__ = ['Schema']

import os
import sys
import yaml
from collections import OrderedDict

from .column import Column

class Schema(object):
    def __init__(self, table):
        self.table = table
        self.type_fields = None

    def __getitem__(self, key):
        return self.type_fields[key]

    @classmethod
    def create(cls, db, table_name, type_fields):
        # save schema
        dirpath = os.path.join(db.store.data_path, db.db_name, table_name)
        filename = 'schema.yaml'
        path = os.path.join(dirpath, filename)

        # type_fields
        _type_fields = {}

        for c, t in type_fields.items():
            if c == 'primary_key':
                _type_fields[c] = t
            else:
                _type_fields[c] = t.get_dict()
        
        # save
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
        
        # load
        with open(path, 'rb') as f:
            _type_fields = yaml.load(f)

        # sort type_fields
        type_fields = OrderedDict(
            (c, Column(**t))
            for c, t in sorted(_type_fields.items(), key=lambda n: n[0])
            if c != 'primary_key'
        )

        # add primary_key at the end of dict
        type_fields['primary_key'] = _type_fields['primary_key']

        self.type_fields = type_fields
