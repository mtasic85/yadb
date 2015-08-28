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
        self.opened = False
        self.databases = {}
        self.transactions = defaultdict(deque)
        self.commiting_transactions = set()
        self.check_lock = threading.Lock()

    def __enter__(self):
        # open self if not
        if not self.is_opened():
            self.open()

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.is_opened():
            self.close()

    def get_store_path(self):
        return self.data_path

    def is_opened(self):
        return self.opened

    def open(self):
        self.opened = True

    def close(self):
        for db_name, db in self.databases.items():
            if db.is_opened():
                db.close()

        self.opened = False

    def database(self, db_name):
        # open self if not
        if not self.is_opened():
            self.open()

        db = Database(self, db_name)
        self.databases[db_name] = db
        return db

    @property
    def tx(self):
        # open self if not
        if not self.is_opened():
            self.open()

        tx = Transaction(self)
        return tx

    def get_current_transaction(self):
        # currently running transaction in current thread
        tx_queue = self.transactions[thread.get_ident()]
        tx = tx_queue[-1]
        return tx
