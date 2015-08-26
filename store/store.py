__all__ = ['Store']

import os
import sys
import thread
import threading
from collections import defaultdict, deque

from .database import Database
from .transaction import Transaction

class Store(object):
    def __init__(self, data_path=None):
        self.data_path = data_path
        self.databases = []
        self.transactions = defaultdict(deque)
        self.commiting_transactions = set()
        self.check_lock = threading.Lock()

    def close(self):
        for db in self.databases:
            db.close()

    def database(self, db_name):
        db = Database.create(self, db_name)
        self.databases.append(db)
        return db

    @property
    def tx(self):
        tx = Transaction(self)
        return tx

    def get_current_transaction(self):
        # currently running transaction in current thread
        tx = self.transactions[thread.get_ident()][-1]
        return tx
