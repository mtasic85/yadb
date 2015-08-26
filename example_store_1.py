from pprint import pprint
from store import Store

s = Store(data_path='data')
db1 = s.create_database('db1')
t1 = db1.create_table('t1', a='int', b='str', c='float', primary_key=['a', 'c'])

with s.tx:
    for i in range(100):
        for j in range(100):
            t1.insert(a=i, b='2', c=float(j))
    
    # a = t1.get(1, 3.0)
    # b = t1.get(10, 30.0)
    # c = t1.query.select('b', 'c').where(t1.a <= 1 and t1.a >= 10).one()

# print a.get()
# print b.get()
# pprint(t1.memtable.get_sorted_rows())

# print t1.sstables[0].index.get({'a': 1, 'c': 1.0})
print t1.sstables[0].get((1, 1.0))
print t1.sstables[0].get((2, 7.0))