__all__ = ['Deferred']

class Deferred(object):
    def __init__(self, value=None, query=None):
        self._value = value
        self._query = query

    def set(self, value):
        self._value = value

    def get(self):
        return self._value
