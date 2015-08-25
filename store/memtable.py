__all__ = ['MemTable']

from collections import OrderedDict

class MemTable(OrderedDict):
    def __init__(self, table, *args, **kwargs):
        super(MemTable, self).__init__(*args, **kwargs)
        self.table = table

    def get_sorted_rows(self):
        rows = self.values()
        
        rows.sort(key=lambda row: (
            row[c] for c in self.table.schema.type_fields['primary_key']
        ))

        return rows