__all__ = ['Deferred']

class Deferred(object):
    def __init__(self):
        self._value = None

    def set(self, value):
        self._value = value

    def get(self):
        return self._value
