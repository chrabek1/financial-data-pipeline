from abc import ABC, abstractmethod

class DataSource(ABC):
    @abstractmethod
    def fetch_daily(self, symbol: str) -> dict:
        pass
    