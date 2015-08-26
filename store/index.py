__all__ = ['Index']

import os
import sys
import mmap
import struct

class Index(object):
    def __init__(self, sstable, path):
        self.sstable = sstable
        self.path = path
        self.mm = None
        self.f = None

    @classmethod
    def _get_key_packed(cls, table, row, pos):
        key_blob_items = []
        
        for c in table.schema.type_fields['primary_key']:
            t = table.schema.type_fields[c]
            b = t._get_column_packed(row[c])
            key_blob_items.append(b)

        key_blob = b''.join(key_blob_items) 
        pos_blob = struct.pack('!Q', pos)
        key_blob += pos_blob
        return key_blob

    @classmethod
    def _get_key_size(cls, table, key):
        size = 0

        for c in table.schema.type_fields['primary_key']:
            t = table.schema.type_fields[c]
            i = table.schema.type_fields['primary_key'].index(c)
            s = t._get_column_size(key[i])
            size += s

        return size

    @classmethod
    def _get_key_unpacked(cls, table, mm, pos):
        key = []
        p = pos

        for c in table.schema.type_fields['primary_key']:
            t = table.schema.type_fields[c]
            
            # print c, p
            v, p = t._get_column_unpacked(mm, p)
            key.append(v)

        sstable_pos, = struct.unpack_from('!Q', mm, p)
        # p += 8

        key = tuple(key)
        return key, sstable_pos

    def open(self):
        self.f = open(self.path, 'r+b')
        self.mm = mmap.mmap(self.f.fileno(), 0)

    def close(self):
        self.mm.close()
        self.f.close()
    
    def _get_sstable_pos(self, key):
        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low <= high:
            mid = (low + high) // 2

            key_pos = mid * step
            cur_key, sstable_pos = Index._get_key_unpacked(table, self.mm, key_pos)
            print 'cur_key:', cur_key

            if cur_key > key:
                high = mid - 1
            elif cur_key < key:
                low = mid + 1
            else:
                break
        else:
            sstable_pos = None

        return sstable_pos