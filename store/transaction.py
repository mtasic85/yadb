__all__ = ['Transaction']

class Transaction(object):
    def __init__(self, store):
        self.store = store
        self.read_log = []
        self.write_log = []
    
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False
