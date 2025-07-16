# acoustic_word → {audio_id: freq}
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import numpy as np
import pickle
from tqdm import tqdm

from indexes.audioindex import AudioInvertedIndex  # lo haremos ahora

K = 100
ROOT = "F:/spotify_songs/dataset"
HIST_PATH = os.path.join(ROOT, f"histograms_audio_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"audio_index_k{K}.pkl")

with open(HIST_PATH, "rb") as f:
    histograms = pickle.load(f)

index = AudioInvertedIndex()

for audio_id, hist in tqdm(histograms.items()):
    for word_id, freq in enumerate(hist):
        if freq > 0:
            index.insert(word_id, audio_id, int(freq))

index.save(INDEX_PATH)
print(f"✅ Índice invertido acústico guardado en: {INDEX_PATH}")
