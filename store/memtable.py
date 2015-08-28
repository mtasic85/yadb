__all__ = ['MemTable']

import bisect

class MemTable(object):
    def __init__(self, table, *args, **kwargs):
        self.table = table
        self.items = []

        for k, row in args:
            self.items.append(k, row)

        for k, row in kwargs.items():
            self.items.append(k, row)

        self.items.sort(key=lambda n: n[0])

    def __len__(self):
        return len(self.items)

    def get(self, key):
        pos = None
        row = None

        for i, (k, r) in enumerate(self.items):
            if k == key:
                pos = i
                row = r
                break
        else:
            raise KeyError

        return row, pos, pos

    def set(self, key, row):
        bisect.insort(self.items, (key, row))

    def get_lt(self, key):
        keys = tuple(k for k, r in self.items)
        memtable_pos = bisect.bisect_left(keys, key)

        if memtable_pos:
            row = self.items[memtable_pos - 1][1]
        else:
            raise KeyError

        return row, memtable_pos

    def get_le(self, key):
        keys = tuple(k for k, r in self.items)
        memtable_pos = bisect.bisect_right(keys, key)

        if memtable_pos:
            row = self.items[memtable_pos - 1][1]
        else:
            raise KeyError

        return row, memtable_pos

    def get_gt(self, key):
        keys = tuple(k for k, r in self.items)
        memtable_pos = bisect.bisect_right(keys, key)

        if memtable_pos != len(keys):
            row = self.items[memtable_pos][1]
        else:
            raise KeyError

        return row, memtable_pos

    def get_ge(self, key):
        keys = tuple(k for k, r in self.items)
        memtable_pos = bisect.bisect_left(keys, key)

        if memtable_pos != len(keys):
            row = self.items[memtable_pos][1]
        else:
            raise KeyError

        return row, memtable_pos

    def get_sorted_rows(self, columns):
        '''
        sort by table's primary_key
        '''
        rows = list(r for k, r in self.items)
        rows.sort(key=lambda row: tuple(row[c] for c in columns))
        return rows
