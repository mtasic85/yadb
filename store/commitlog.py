__all__ = ['CommitLog']

from index import Index
from sstable import SSTable

class CommitLog(object):
    def __init__(self, table):
        self.table = table
        self.index = Index(table)
        self.sstable = SSTable(table)

    def load(self, filename):
        self.index.load(filename)
        self.sstable.load(filename)

    def save(self):
        # FIXME:
        rows = dict(self.table.memtable)
        self.sstable.set_rows(rows)
        self.sstable.save()
