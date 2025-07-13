import pickle
from typing import Dict, Set, List, Optional


class VisualInvertedIndex:
    def __init__(self):
        # word_id → {img_id: freq}
        self.index: Dict[int, Dict[int, int]] = {}

    def insert(self, word_id: int, img_id: int, freq: int):
        if word_id not in self.index:
            self.index[word_id] = {}
        self.index[word_id][img_id] = freq

    def save(self, path: str):
        with open(path, 'wb') as f:
            pickle.dump(self.index, f)

    @classmethod
    def load(cls, path: str) -> 'VisualInvertedIndex':
        with open(path, 'rb') as f:
            data = pickle.load(f)
        obj = cls()
        obj.index = data
        return obj

    def get_candidates(self, word_ids: List[int]) -> Set[int]:
        """
        Devuelve el conjunto de imágenes candidatas que contienen al menos
        una palabra visual del query.
        """
        candidates: Set[int] = set()
        for word_id in word_ids:
            if word_id in self.index:
                candidates.update(self.index[word_id].keys())
        return candidates

    def get_histogram(self, word_ids: List[int], img_id: int, k: int) -> List[float]:
        """
        Retorna el histograma para una imagen candidata restringido
        a las palabras visuales del query.
        """
        hist = [0.0] * k
        for word_id in word_ids:
            if word_id in self.index and img_id in self.index[word_id]:
                hist[word_id] = float(self.index[word_id][img_id])
        return hist
