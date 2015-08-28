from pprint import pprint
from store import Store

s = Store('data')
db1 = s.database('db1')
t1 = db1.table('t1', a='int', b='str', c='float', primary_key=['a', 'c'])

with s.tx:
    for i in range(10):
        for j in range(10):
            t1.insert(a=i, b='2', c=float(j))
    
    a = t1.get(1, 3.0)
    b = t1.get(2, 9.0)

    q = t1.select('b', 'c')
    q = q.where(t1.a >= 1, t1.a < 9, t1.c == 5.0)
    r = q.all()

print a.get()
print b.get()
print r.get()

s.close()