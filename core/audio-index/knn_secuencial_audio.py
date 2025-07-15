import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

import heapq
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from preprocessing.audio.audio_utils import (
    extract_mfcc_from_wav,
    build_histogram_from_mfcc,
    tfidf_transform,
    load_centroids,
    load_tfidf_index
)

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
K = 100
QUERY_PATH = "F:/spotify_songs/dataset/The-Weeknd-Blinding-Lights-.wav"  # ⚠️ cambia esto por tu archivo de entrada
TOP_K = 5

# --- Cargar modelo e índice ---
centroids = load_centroids()
tfidf_index = load_tfidf_index()
idf_vector = np.log((1 + len(tfidf_index)) / (1 + np.sum([v > 0 for v in np.array(list(tfidf_index.values()))], axis=0))) + 1

# --- Procesar query ---
mfcc = extract_mfcc_from_wav(QUERY_PATH)
hist = build_histogram_from_mfcc(mfcc, centroids)
query_vec = tfidf_transform(hist, idf_vector).reshape(1, -1)

# --- Comparar con todos ---
heap = []
for audio_id, vec in tfidf_index.items():
    sim = cosine_similarity(query_vec, vec.reshape(1, -1))[0][0]
    heapq.heappush(heap, (sim, audio_id))

top = heapq.nlargest(TOP_K, heap)

# --- Mostrar resultados ---
print(f"\n🔊 Top {TOP_K} audios similares:")
for sim, audio_id in top:
    print(f"🎵 {audio_id} | Score: {sim:.4f}")
