import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import numpy as np
import pickle
from sklearn.metrics.pairwise import cosine_similarity
from preprocessing.audio.audio_utils import (
    extract_mfcc_from_wav, build_histogram_from_mfcc, tfidf_transform, load_centroids
)
from indexes.audioindex import AudioInvertedIndex
import heapq

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
K = 100
QUERY_PATH = os.path.join(ROOT, "The-Weeknd-Blinding-Lights-.wav")
TFIDF_PATH = os.path.join(ROOT, f"tfidf_audio_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"audio_index_k{K}.pkl")

# --- Cargar TF-IDF index ---
with open(TFIDF_PATH, "rb") as f:
    tfidf_index = pickle.load(f)

idf_vector = np.log((1 + len(tfidf_index)) / (1 + np.sum([v > 0 for v in np.array(list(tfidf_index.values()))], axis=0))) + 1

# --- Cargar índice invertido ---
index = AudioInvertedIndex.load(INDEX_PATH)

# --- Preparar query ---
centroids = load_centroids()
mfcc = extract_mfcc_from_wav(QUERY_PATH)
query_hist = build_histogram_from_mfcc(mfcc, centroids)
query_tfidf = tfidf_transform(query_hist, idf_vector).reshape(1, -1)

# --- Palabras activas en la query ---
nonzero_words = np.nonzero(query_hist)[0]
candidates = index.get_candidates(nonzero_words)

# --- Comparar solo con candidatos ---
results = []
for audio_id in candidates:
    vec = tfidf_index.get(audio_id)
    if vec is not None:
        sim = cosine_similarity(query_tfidf, vec.reshape(1, -1))[0][0]
        results.append((sim, audio_id))

top = heapq.nlargest(5, results)

print("\n🔊 Resultados más similares (invertido):")
for sim, audio_id in top:
    print(f"🎵 {audio_id} | Score: {sim:.4f}")
