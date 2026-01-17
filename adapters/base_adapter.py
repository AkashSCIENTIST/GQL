from abc import ABC, abstractmethod

class BaseAdapter(ABC):
    @abstractmethod
    def execute(self, query_ast):
        """Must return a list of dictionaries (records)."""
        pass