__all__ = ['Column']

import struct

class Column(object):
    def __init__(self, name=None, type=None, size=None):
        self.name = name
        self.type = type
        self.size = size

    def get_dict(self):
        return {
            'name': self.name,
            'type': self.type,
            'size': self.size,
        }

    def _get_struct_format(self, value=None):
        if self.type == 'bool':
            fmt = b'BB'
        elif self.type == 'int':
            fmt = b'Bq'
        elif self.type == 'float':
            fmt = b'Bd'
        elif self.type == 'str':
            if value is None:
                fmt = b'BQ%is' % self.size
            else:
                fmt = b'BQ%is' % len(value)
        else:
            raise Exception('unsupported column type')

        return fmt

    def _get_column_struct_packed(self, value):
        fmt = self._get_struct_format(value)
        is_null = 1 if value is None else 0
        b = struct.pack(fmt, is_null, value)
        return b
