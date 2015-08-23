__all__ = ['Transaction']

import threading

class Transaction(object):
    def __init__(self, store):
        self.store = store
        self.read_log = []
        self.write_log = []

    def __enter__(self):
        # thread local transaction
        local = threading.local()
        local._yadb_store_transaction = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        local._yadb_store_transaction = None
        return False
