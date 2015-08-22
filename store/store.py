import os
import sys

from .database import Database

class Store(object):
    def __init__(self, data_path=None):
        self.data_path = data_path

    def create_database(self, db_name):
        db = Database.create(self, db_name)
        return db
