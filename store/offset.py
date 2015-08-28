__all__ = ['Offset']

import os
import sys
import mmap
import struct

class Offset(object):
    '''
    Offset is class that finds out position of row at given index
    '''

    def __init__(self, sstable, path):
        self.sstable = sstable
        self.path = path
        self.mm = None
        self.f = None

    def __getitem__(self, i):
        offset_pos = i * 8
        sstable_pos, = struct.unpack_from('!Q', self.mm, offset_pos)
        return sstable_pos

    def open(self):
        self.f = open(self.path, 'r+b')
        self.mm = mmap.mmap(self.f.fileno(), 0)

    def close(self):
        self.mm.close()
        self.f.close()

    @classmethod
    def _get_sstable_pos_packed(cls, table, sstable_pos):
        sstable_pos_blob = struct.pack('!Q', sstable_pos)
        return sstable_pos_blob
