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
            v, p = t._get_column_unpacked(mm, p)
            key.append(v)

        sstable_pos, = struct.unpack_from('!Q', mm, p)
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
            # print 'cur_key:', cur_key

            if cur_key > key:
                high = mid - 1
            elif cur_key < key:
                low = mid + 1
            else:
                break
        else:
            sstable_pos = None

        return sstable_pos

    def _get_left_sstable_pos(self, key):
        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2

            key_pos = mid * step
            cur_key, sstable_pos = Index._get_key_unpacked(table, self.mm, key_pos)
            # print 'left cur_key:', cur_key
            
            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)
            
            if _cur_key < _key:
                low = mid + 1
            else:
                high = mid
        
        _, sstable_pos = Index._get_key_unpacked(table, self.mm, low * step)
        return sstable_pos

    def _get_right_sstable_pos(self, key):
        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2

            key_pos = mid * step
            cur_key, sstable_pos = Index._get_key_unpacked(table, self.mm, key_pos)
            # print 'left cur_key:', cur_key

            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)

            if _key < _cur_key:
                high = mid
            else:
                low = mid + 1
        
        _, sstable_pos = Index._get_key_unpacked(table, self.mm, low * step)
        return sstable_pos

    def _get_lt_sstable_pos(self, key):
        # table = self.sstable.table
        # step = Index._get_key_size(table, key) + 8
        # sstable_pos = self._get_left_sstable_pos(key) - step
        # return sstable_pos

        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2

            key_pos = mid * step
            cur_key, sstable_pos = Index._get_key_unpacked(table, self.mm, key_pos)
            # print 'left cur_key:', cur_key
            
            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)
            
            if _cur_key < _key:
                low = mid + 1
            else:
                high = mid
        
        _, sstable_pos = Index._get_key_unpacked(table, self.mm, (low - 1) * step)
        return sstable_pos

    def _get_le_sstable_pos(self, key):
        # table = self.sstable.table
        # step = Index._get_key_size(table, key) + 8
        # sstable_pos = self._get_right_sstable_pos(key) - step
        # return sstable_pos

        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2

            key_pos = mid * step
            cur_key, sstable_pos = Index._get_key_unpacked(table, self.mm, key_pos)
            # print 'left cur_key:', cur_key

            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)

            if _key < _cur_key:
                high = mid
            else:
                low = mid + 1
        
        _, sstable_pos = Index._get_key_unpacked(table, self.mm, (low - 1) * step)
        return sstable_pos

    def _get_gt_sstable_pos(self, key):
        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8
        sstable_pos = self._get_right_sstable_pos(key)
        return sstable_pos

    def _get_ge_sstable_pos(self, key):
        table = self.sstable.table
        step = Index._get_key_size(table, key) + 8
        sstable_pos = self._get_left_sstable_pos(key)
        return sstable_pos
