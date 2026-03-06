from abc import ABC, abstractmethod
import pandas as pd

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, payload: dict, symbol: str) -> pd.DataFrame:
        pass
    