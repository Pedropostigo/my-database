from dataclasses import dataclass

@dataclass
class Column:
    type:str
    column_name:str

    def to_dict(self):
        return {
            'type': self.type,
            'column_name': self.column_name
        }