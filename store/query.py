
class Query(object):
    def __init__(self, store):
        self.store = store

    def select(self, *args):
        return self

    def where(self, *args):
        return self

    def one(self):
        row = None
        return row

    def all(self):
        rows = []
        return rows
