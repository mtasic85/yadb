__all__ = ['Index']

import os
import sys
import mmap
import struct

class Index(object):
    def __init__(self, sstable, t, columns, path=None):
        self.sstable = sstable
        self.t = t
        self.columns = columns
        self.mm = None
        self.f = None

    def get_path(self):
        filename = 'index-%s-%s.data' % (self.t, '-'.join(self.columns))
        path = os.path.join(self.sstable.table.get_path(), filename)
        return path

    def open(self):
        '''
        Open file for reading.
        '''
        self.f = open(self.get_path(), 'r+b')
        self.mm = mmap.mmap(self.f.fileno(), 0)

    def close(self):
        '''
        Open file for reading.
        '''
        self.mm.close()
        self.f.close()

    def w_open(self):
        '''
        Open file for writing.
        '''
        self.f = open(self.get_path(), 'wb')

    def w_close(self):
        '''
        Close file for writing.
        '''
        self.f.close()

    def _write_key(self, row, sstable_pos):
        table = self.sstable.table
        key_blob_items = []
        
        for c in table.schema.primary_key:
            t = table.schema[c]
            b = t._get_column_packed(row[c])
            key_blob_items.append(b)

        key_blob = b''.join(key_blob_items) 
        pos_blob = struct.pack('!Q', sstable_pos)
        self.f.write(key_blob)
        self.f.write(pos_blob)

    def _read_key(self, pos):
        table = self.sstable.table
        key = []
        p = pos

        for c in table.schema.primary_key:
            t = table.schema[c]
            v, p = t._get_column_unpacked(self.mm, p)
            key.append(v)

        key = tuple(key)
        sstable_pos, = struct.unpack_from('!Q', self.mm, p)
        return key, sstable_pos

    def _get_key_size(self, key):
        table = self.sstable.table
        size = 0

        for c in table.schema.primary_key:
            t = table.schema[c]
            i = table.schema.primary_key.index(c)
            s = t._get_column_size(key[i])
            size += s

        return size

    def get_sstable_pos(self, key):
        sstable = self.sstable
        table = self.sstable.table
        step = 8 + self._get_key_size(key) # !Q + KEY

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None
        offset_pos = None

        while low <= high:
            mid = (low + high) // 2
            key_pos = mid * step
            offset_pos = mid
            cur_key, sstable_pos = self._read_key(key_pos)
            # print 'cur_key:', cur_key

            if cur_key > key:
                high = mid - 1
            elif cur_key < key:
                low = mid + 1
            else:
                break
        else:
            sstable_pos = None

        return offset_pos, sstable_pos
    
    def get_lt_sstable_pos(self, key):
        sstable = self.sstable
        table = self.sstable.table
        step = 8 + self._get_key_size(key) # !Q + KEY

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2
            key_pos = mid * step
            cur_key, sstable_pos = self._read_key(key_pos)
            # print 'left cur_key:', cur_key
            
            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)
            
            if _cur_key < _key:
                low = mid + 1
            else:
                high = mid
        
        offset_pos = low - 1
        _, sstable_pos = self._read_key(offset_pos * step)
        return offset_pos, sstable_pos

    def get_le_sstable_pos(self, key):
        sstable = self.sstable
        table = self.sstable.table
        step = 8 + self._get_key_size(key) # !Q + KEY

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2
            key_pos = mid * step
            cur_key, sstable_pos = self._read_key(key_pos)
            # print 'left cur_key:', cur_key

            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)

            if _key < _cur_key:
                high = mid
            else:
                low = mid + 1
        
        offset_pos = low - 1
        _, sstable_pos = self._read_key(offset_pos * step)
        return offset_pos, sstable_pos

    def get_gt_sstable_pos(self, key):
        sstable = self.sstable
        table = self.sstable.table
        step = 8 + self._get_key_size(key) # !Q + KEY

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2
            key_pos = mid * step
            cur_key, sstable_pos = self._read_key(key_pos)
            # print 'left cur_key:', cur_key

            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)

            if _key < _cur_key:
                high = mid
            else:
                low = mid + 1
        
        offset_pos = low
        _, sstable_pos = self._read_key(offset_pos * step)
        return offset_pos, sstable_pos

    def get_ge_sstable_pos(self, key):
        sstable = self.sstable
        table = self.sstable.table
        step = 8 + self._get_key_size(key) # !Q + KEY

        # binary search
        low = 0
        high = (self.mm.size() // step) - 1
        key_pos = None

        while low < high:
            mid = (low + high) // 2
            key_pos = mid * step
            cur_key, sstable_pos = self._read_key(key_pos)
            # print 'left cur_key:', cur_key
            
            _key = tuple(x for x, y in zip(key, cur_key) if x != None)
            _cur_key = tuple(y for x, y in zip(key, cur_key) if x != None)
            
            if _cur_key < _key:
                low = mid + 1
            else:
                high = mid
        
        offset_pos = low
        _, sstable_pos = self._read_key(offset_pos * step)
        return offset_pos, sstable_pos
