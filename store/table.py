__all__ = ['Table']

import os
import sys
from pprint import pprint
from collections import defaultdict

from .column import Column
from .schema import Schema
from .memtable import MemTable
from .sstable import SSTable
from .query import Query
from .deferred import Deferred
from .expr import Expr

class Table(object):
    MEMTABLE_LIMIT_N_ITEMS = 100

    def __init__(self, db, table_name, type_fields=None):
        self.store = db.store
        self.db = db
        self.table_name = table_name
        self.opened = False

        # schema
        self.schema = Schema(self, type_fields)

        # memtable
        self.memtable = MemTable(self)

        # sstables
        self.sstables = []
        table_path = self.get_path()

        for filename in os.listdir(table_path):
            if not filename.startswith('sstable-'):
                continue

            s = filename.index('sstable-') + len('sstable-')
            e = filename.index('.data')
            t = filename[s:e]

            # sstable
            sst = SSTable(self, t)
            sst.open() # FIXME: lazy open in SSTable only if required
            self.sstables.append(sst)

    def __getattr__(self, attr):
        c = getattr(self.schema, attr)
        return c

    def get_path(self):
        return os.path.join(self.db.get_path(), self.table_name)

    def is_opened(self):
        return self.opened

    def open(self):
        self.opened = True

    def close(self):
        for sst in self.sstables:
            if sst.is_opened():
                sst.close()

        self.opened = False

    def commit_if_required(self):
        if len(self.memtable) >= self.MEMTABLE_LIMIT_N_ITEMS:
            self.commit()

    def commit(self):
        # get sorted rows by primary_key
        columns = self.schema.primary_key
        rows = self.memtable.get_sorted_rows(columns)
        
        # create new sstable
        sst = SSTable(self, rows=rows)
        sst.open()
        self.sstables.append(sst)

        # clear memtable
        self.memtable = MemTable(self)

    @property
    def query(self):
        q = Query(self.db.store)
        return q

    def insert(self, **row):
        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self, Table._commit_insert, (self,), row))

    def _commit_insert(self, **row):
        # check if all columns exist in table's schema
        # compare against schema
        for k, v in row.items():
            if k not in self.schema:
                raise Exception(
                    'filed %r is not defined in schema for table %r' % (
                        k,
                        self.table_name,
                    )
                )

        # set default columns to None
        for k, v in self.schema:
            if k not in row:
                row[k] = None

        # build key
        key = tuple(row[k] for k in self.schema.primary_key)

        # insert key
        self.memtable.set(key, row)

        # commit if required
        self.commit_if_required()

    def get(self, *args):
        # key
        key = args

        # deferred
        d = Deferred()

        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self.table, Table._commit_get, (self, d, key), {}))

        return d
    
    def _commit_get(self, d, key):
        v, op, sp = self._get(key)
        d.set(v)

    def select(self, *args):
        # deferred, queue
        d = Deferred()
        q = Query(self.store, d)

        # tx
        tx = self.store.get_current_transaction()
        tx.log((self.db, self.table, Table._commit_select, (self, d, q), {}))

        return q

    """
    def _eval_expr(self, expr):
        print '_eval_expr:', expr
        ranges = []

        if isinstance(expr.left, Expr):
            l_ranges = self._eval_expr(expr.left)
        else:
            l_ranges = None

        if isinstance(expr.right, Expr):
            r_ranges = self._eval_expr(expr.right)
        else:
            r_ranges = None

        if isinstance(expr.left, Column) and r_ranges is None:
            k = (expr.right,)
            cs = (expr.left.name,)

            if expr.op == '==':
                v, op, sp = self._get(k, cs)
                ranges.append(((cs, expr.op, k), None))
            elif expr.op == '<':
                v, op, sp = self._get_lt(k, cs)
                ranges.append((None, (cs, expr.op, k)))
            elif expr.op == '<=':
                v, op, sp = self._get_le(k, cs)
                ranges.append((None, (cs, expr.op, k)))
            elif expr.op == '>':
                v, op, sp = self._get_gt(k, cs)
                ranges.append(((cs, expr.op, k), None))
            elif expr.op == '>=':
                v, op, sp = self._get_ge(k, cs)
                ranges.append(((cs, expr.op, k), None))
            # print 'v, op, sp:', v, op, sp
        elif isinstance(expr.left, Column) and l_ranges is None:
            k = (expr.right,)
            cs = (expr.left.name,)

            if expr.op == '==':
                v, op, sp = self._get(k, cs)
                ranges.append(((cs, expr.op, k), None))
            elif expr.op == '<':
                v, op, sp = self._get_lt(k, cs)
                ranges.append((None, (cs, expr.op, k)))
            elif expr.op == '<=':
                v, op, sp = self._get_le(k, cs)
                ranges.append((None, (cs, expr.op, k)))
            elif expr.op == '>':
                v, op, sp = self._get_gt(k, cs)
                ranges.append(((cs, expr.op, k), None))
            elif expr.op == '>=':
                v, op, sp = self._get_ge(k, cs)
                ranges.append(((cs, expr.op, k), None))
            # print 'v, op, sp:', v, op, sp
        else:
            ranges_by_column = defaultdict(list)

            for l_range, r_range in l_ranges:
                if l_range is not None:
                    c, op, v = l_range
                    r = (l_range, r_range)

                    if r not in ranges_by_column[c]:
                        ranges_by_column[c].append(r)

            for l_range, r_range in r_ranges:
                if l_range is not None:
                    c, op, v = l_range
                    r = (l_range, r_range)

                    if r not in ranges_by_column[c]:
                        ranges_by_column[c].append(r)

            print 'ranges_by_column[1]:'
            pprint(ranges_by_column)

            if expr.op == 'and':
                # if '==' is for same column
                # then ignore all other operation
                #
                # eg: a > 1 and a == 5 is always a == 5
                for c, rs in ranges_by_column.items():
                    for r in rs[:]:
                        l_range, r_range = r

                        if l_range:
                            c, op, v = l_range

                            if op == '==':
                                ranges_by_column[c] = [r]
                                break

                for c, rs in ranges_by_column.items():
                    for r in rs:
                        if r not in ranges:
                            ranges.append(r)
            elif expr.op == 'or':
                pass

            print 'ranges_by_column[2]:'
            pprint(ranges_by_column)

            # if expr.op == '==':
            #     pass
            # elif expr.op == '<':
            #     pass
            # elif expr.op == '<=':
            #     pass
            # elif expr.op == '>':
            #     pass
            # elif expr.op == '>=':
            #     pass

        return ranges
    """

    """
    def _cmp_op_val(self, a, b):
        aop, av = a
        bop, bv = b



    def _cmp_ranges(self, a, b):
        op_map = {
            '<': -2,
            '<=': -1,
            '==': 0,
            '>=': 1,
            '>': 2,
        }

        al_range, ar_range = a
        bl_range, br_range = b
        # alc, alop, alv = al_range
        # arc, arop, arv = ar_range
        # blc, blop, blv = bl_range
        # brc, brop, brv = br_range

        if al_range and not ar_range:
            alc, alop, alv = al_range
            
            if bl_range and not br_range:
                blc, blop, blv = bl_range
            elif not bl_range and br_range:
                brc, brop, brv = br_range
                
                if op_map[alop] < op_map[brop]:
                    return -1
                elif op_map[alop] > op_map[brop]:
                    if alv < brv:
                        print '!', a, b
                        return -1
                    else:
                        return 1
                elif op_map[alop] == op_map[brop]:
                    return 0
            else:
                raise ValueError
        elif not al_range and ar_range:
            arc, arop, arv = ar_range

            if bl_range and not br_range:
                blc, blop, blv = bl_range
                # return -1
            elif not bl_range and br_range:
                brc, brop, brv = br_range
            else:
                raise ValueError
        else:
            raise ValueError

        return 0

    def _eval_expr(self, expr):
        print '_eval_expr:', expr
        ranges = []

        if expr.op == 'and':
            l_ranges = self._eval_expr(expr.left)
            r_ranges = self._eval_expr(expr.right)
            
            _ranges = list(set(l_ranges + r_ranges))
            # _ranges.sort(key=lambda rs: self._sort_ranges(*rs))
            _ranges.sort(cmp=lambda a, b: self._cmp_ranges(a, b))
            print '_ranges:', _ranges

            # _ranges = []

            # for l_range in l_ranges:
            #     lc, lop, lv = l_range

            #     for r_range in r_ranges:
            #         rc, rop, rv = r_range

        elif expr.op == 'or':
            # FIXME: implement OR
            pass
        else:
            k = (expr.right,)
            cs = (expr.left.name,)

            if expr.op == '==':
                v, op, sp = self._get(k, cs)
                ranges.append(((cs, expr.op, k), None))
            elif expr.op == '<':
                v, op, sp = self._get_lt(k, cs)
                ranges.append((None, (cs, expr.op, k)))
            elif expr.op == '<=':
                v, op, sp = self._get_le(k, cs)
                ranges.append((None, (cs, expr.op, k)))
            elif expr.op == '>':
                v, op, sp = self._get_gt(k, cs)
                ranges.append(((cs, expr.op, k), None))
            elif expr.op == '>=':
                v, op, sp = self._get_ge(k, cs)
                ranges.append(((cs, expr.op, k), None))

        return ranges
    """

    def _cmp_ranges(self, a, b):
        op_map = {
            '<': -2,
            '<=': -1,
            '==': 0,
            '>=': 1,
            '>': 2,
        }

        ac, aop, av = a
        bc, bop, bv = b

        if ac == bc:
            if aop in ('<', '<=') and bop in ('<', '<='):
                return cmp(av, bv)
            elif aop in ('>=', '>') and bop in ('>', '>='):
                return cmp(av, bv)
            elif aop in ('>=', '>') and bop in ('<', '<='):
                if av < bv:
                    return -1
                elif av > bv:
                    return 1
                else:
                    return 0
            elif aop in ('<', '<=') and bop in ('>=', '>'):
                if av < bv:
                    return 0
                elif av > bv:
                    return 1
                else:
                    return 0
            else:
                return 0
        else:
            return cmp(ac, bc)

    def _eval_expr(self, expr):
        print '_eval_expr:', expr
        ranges = []
        
        if expr.op == 'and':
            l_ranges = self._eval_expr(expr.left)
            r_ranges = self._eval_expr(expr.right)
            _ranges = list(set(l_ranges + r_ranges))
            _ranges.sort(cmp=lambda a, b: self._cmp_ranges(a, b))
            
            # for same column, if op is '==',
            # then it cancels all other ops if found inside of a range

            print '_ranges:', _ranges
            ranges.extend(_ranges)
        elif expr.op == 'or':
            # FIXME: implement OR
            pass
        else:
            k = (expr.right,)
            cs = (expr.left.name,)

            if expr.op == '==':
                v, op, sp = self._get(k, cs)
            elif expr.op == '<':
                v, op, sp = self._get_lt(k, cs)
            elif expr.op == '<=':
                v, op, sp = self._get_le(k, cs)
            elif expr.op == '>':
                v, op, sp = self._get_gt(k, cs)
            elif expr.op == '>=':
                v, op, sp = self._get_ge(k, cs)
            
            ranges.append((cs[0], expr.op, k[0]))

        return ranges

    def _commit_select(self, d, q):
        rows = []   # get from keys
        keys = []   # get from ranges
        ranges = self._eval_expr(q.where_clause)
        print 'ranges:', ranges
        d.set(rows)

    def _get(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp

    def _get_lt(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_lt(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_lt(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp

    def _get_le(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_le(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_le(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp
    
    def _get_gt(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_gt(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_gt(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp
    
    def _get_ge(self, key, columns=None):
        try:
            v, op, sp = self.memtable.get_ge(key, columns)
        except KeyError as e:
            for sst in reversed(self.sstables):
                try:
                    v, op, sp = sst.get_ge(key, columns)
                    break
                except KeyError as e:
                    pass
            else:
                raise KeyError

        return v, op, sp
