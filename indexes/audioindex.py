import pickle
from typing import Dict, List, Set

class AudioInvertedIndex:
    def __init__(self):
        self.index: Dict[int, Dict[str, int]] = {}

    def insert(self, word_id: int, audio_id: str, freq: int):
        if word_id not in self.index:
            self.index[word_id] = {}
        self.index[word_id][audio_id] = freq

    def get_candidates(self, word_ids: List[int]) -> Set[str]:
        candidates = set()
        for word in word_ids:
            if word in self.index:
                candidates.update(self.index[word].keys())
        return candidates

    def save(self, path: str):
        with open(path, "wb") as f:
            pickle.dump(self.index, f)

    @classmethod
    def load(cls, path: str):
        with open(path, "rb") as f:
            data = pickle.load(f)
        obj = cls()
        obj.index = data
        return obj
