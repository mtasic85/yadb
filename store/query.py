__all__ = ['Query']

from .deferred import Deferred

class Query(object):
    def __init__(self, store, d):
        self.store = store
        self.d = d
        self.select_clauses = []
        self.where_clauses = []

    def select(self, *args):
        for select_clause in args:
            self.select_clauses.append(select_clause)

        return self

    def where(self, *args):
        for where_clause in args:
            self.where_clauses.append(where_clause)
        
        return self

    def one(self):
        return self.d

    def all(self):
        return self.d
