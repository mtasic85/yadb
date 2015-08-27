__all__ = ['Expr']

class Expr(object):
    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

    def __repr__(self):
        return '<%s (%r) %s (%r)>' % (
            self.__class__.__name__,
            self.left,
            self.op,
            self.right,
        )

    def __eq__(self, other):
        return Expr(self, '==', other)

    def __ne__(self, other):
        return Expr(self, '!=', other)

    def __lt__(self, other):
        return Expr(self, '<', other)

    def __le__(self, other):
        return Expr(self, '<=', other)

    def __gt__(self, other):
        return Expr(self, '>', other)

    def __ge__(self, other):
        return Expr(self, '>=', other)

if __name__ == '__main__':
    e1 = Expr('a', '>=', 1)
    e2 = Expr('a', '<', 10)
    e3 = Expr(e1, 'and', e2)
    e4 = Expr('c', '==', 5)
    e5 = Expr(e3, 'and', e4)
