__all__ = ['Schema']

import os
import sys
import yaml

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
        
        with open(path, 'wb') as f:
            s = yaml.dump(type_fields)
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
            type_fields = yaml.load(f)

        self.type_fields = type_fields

    def save(self):
        # save schema
        data_path = self.table.db.store.data_path
        db_name = self.table.db.db_name
        table_name = self.table.table_name
        dirpath = os.path.join(data_path, db_name, table_name)
        filename = 'schema.yaml'
        path = os.path.join(dirpath, filename)
        
        with open(path, 'wb') as f:
            s = yaml.dump(type_fields)
            f.write(s)

    def set_type_fields(self, type_fields):
        self.type_fields = type_fields
