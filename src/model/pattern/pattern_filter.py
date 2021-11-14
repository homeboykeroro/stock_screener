from abc import ABC, abstractmethod

class PatternFilter(ABC):
    @abstractmethod
    def filter(self) -> list:
        return NotImplemented