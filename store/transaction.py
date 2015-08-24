__all__ = ['Transaction']

import time
import thread
import threading

class Transaction(object):
    def __init__(self, store):
        self.store = store
        self._log = []

    def __enter__(self):
        # thread local transaction
        self.store.transactions[thread.get_ident()].append(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # thread local transaction
        tx = self.store.transactions[thread.get_ident()].pop()
        self.execute()
        return False

    def log(self, *args):
        self._log.append(args)

    def check(self):
        print 'check:', self
        conflict = False

        with self.store.check_lock:
            for tx in self.store.commiting_transactions:
                for a in self._log:
                    for b in tx._log:
                        # detect conflict
                        if a[0] == b[0] == 'insert':
                            if a[1] == b[1] and a[2] == b[2]:
                                conflict = True
                                break

                    if conflict:
                        break

                if conflict:
                    break

        passed = not conflict
        return passed

    def commit(self):
        print 'commit:', self

    def execute(self):
        print 'execute:', self

        while not self.check():
            time.sleep(0.001)

        self.commit()
