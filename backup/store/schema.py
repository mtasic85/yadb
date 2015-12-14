__all__ = ['Schema']

import os
import sys
import yaml
from collections import OrderedDict

from .column import Column

class Schema(object):
    def __init__(self, table, type_fields=None):
        self.table = table
        
        # schema path
        schema_path = self.get_path()

        # type_fields
        if type_fields and not os.path.exists(schema_path):
            # type_fields
            _type_fields = {}

            for c, t in type_fields.items():
                if c == 'primary_key':
                    _type_fields[c] = t
                else:
                    _type_fields[c] = dict(t)
            
            # save schema
            with open(schema_path, 'wb') as f:
                s = yaml.dump(_type_fields)
                f.write(s)
        elif not type_fields and os.path.exists(schema_path):
            # load schema
            with open(schema_path, 'rb') as f:
                _type_fields = yaml.load(f)

            # sort type_fields
            type_fields = OrderedDict(
                (c, Column(**t))
                for c, t in sorted(_type_fields.items(), key=lambda n: n[0])
                if c != 'primary_key'
            )

            # add primary_key at the end of dict
            type_fields['primary_key'] = _type_fields['primary_key']
        elif type_fields and os.path.exists(schema_path):
            # FIXME: compare given type_fields with schema's type_fields
            pass
        else:
            raise Exception('')

        self.type_fields = type_fields

    def __getitem__(self, key):
        return self.type_fields[key]

    def __getattr__(self, attr):
        return self.type_fields[attr]

    def __contains__(self, n):
        return n in self.type_fields

    def __iter__(self):
        for k, v in self.type_fields.items():
            if k == 'primary_key':
                continue

            yield k, v

    def get_path(self):
        return os.path.join(self.table.get_path(), 'schema.yaml')
