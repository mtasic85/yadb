__all__ = ['Column']

class _Column(object):
    def __init__(self, name=None, type_=None, size=None):
        self.name = name
        self.type = type_
        self.size = size

class Column(object):
    Bool = _Column
    Int = _Column
    Float = _Column
    Str = _Column
