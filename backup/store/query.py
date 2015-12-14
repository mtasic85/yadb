__all__ = ['Query']

from .deferred import Deferred
from .expr import Expr

class Query(object):
    def __init__(self, store, d):
        self.store = store
        self.d = d
        self.select_clauses = []
        self.where_clause = None

    def select(self, *args):
        for select_clause in args:
            self.select_clauses.append(select_clause)

        return self

    def where(self, *args):
        if len(args) == 1:
            self.where_clause = args[0]
            return self

        last_expr = Expr(args[0], 'and', args[1])

        for expr in args[2:]:
            last_expr = Expr(last_expr, 'and', expr)

        self.where_clause = last_expr
        return self

    def one(self):
        return self.d

    def all(self):
        return self.d
