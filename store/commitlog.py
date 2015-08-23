__all__ = ['CommitLog']

from sstable import SSTable

class CommitLog(object):
    def __init__(self, table):
        self.table = table
        self.sstable = SSTable(table)

    def load(self, filename):
        self.sstable.load(filename)

    def save(self):
        rows = dict(self.table.memtable)
        self.sstable.set_rows(rows)
        self.sstable.save()
