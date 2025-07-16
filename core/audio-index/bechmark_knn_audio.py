import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import time
import pickle
import numpy as np
import faiss
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
import heapq

from preprocessing.audio.audio_utils import (
    extract_mfcc_from_wav,
    build_histogram_from_mfcc,
    tfidf_transform,
    load_centroids
)
from indexes.audioindex import AudioInvertedIndex

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
K = 100
QUERY_PATH = os.path.join(ROOT, "The-Weeknd-Blinding-Lights-.wav")

TFIDF_PATH = os.path.join(ROOT, f"tfidf_audio_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"audio_index_k{K}.pkl")

# --- Cargar vectores TF-IDF ---
with open(TFIDF_PATH, "rb") as f:
    tfidf_dict: dict[str, np.ndarray] = pickle.load(f)

audio_ids = sorted(tfidf_dict.keys())
tfidf_matrix = np.stack([tfidf_dict[a] for a in audio_ids]).astype("float32")

# --- Preparar query ---
centroids = load_centroids()
mfcc = extract_mfcc_from_wav(QUERY_PATH)
hist = build_histogram_from_mfcc(mfcc, centroids)
idf = np.log((1 + len(tfidf_dict)) / (1 + (np.array([v > 0 for v in tfidf_dict.values()]).sum(axis=0)))) + 1
query_vec = tfidf_transform(hist, idf).reshape(1, -1).astype("float32")

# --- Cargar índice invertido ---
index = AudioInvertedIndex.load(INDEX_PATH)

# --- Tamaños a evaluar ---
sizes = [1000, 2000, 3000, 4000, 5000, 6000, len(audio_ids)]

print("🎯 Benchmark KNN para Audio\n")

for N in sizes:
    tfidf_subset = tfidf_matrix[:N]
    id_subset = audio_ids[:N]

    # --- Secuencial ---
    start = time.time()
    results = []
    for i in range(N):
        sim = cosine_similarity(query_vec, tfidf_subset[i].reshape(1, -1))[0][0]
        results.append((sim, id_subset[i]))
    top_seq = heapq.nlargest(5, results)
    t_seq = time.time() - start

    # --- Invertido ---
    start = time.time()
    nonzero_words = np.nonzero(hist)[0]
    candidates = list(index.get_candidates(nonzero_words))
    candidate_vecs = []
    candidate_ids = []
    for audio_id in candidates:
        if audio_id in tfidf_dict:
            candidate_ids.append(audio_id)
            candidate_vecs.append(tfidf_dict[audio_id])
    if candidate_vecs:
        candidate_matrix = np.stack(candidate_vecs).astype("float32")
        sims = cosine_similarity(query_vec, candidate_matrix)[0]
        top_inv = heapq.nlargest(5, zip(sims, candidate_ids))
    else:
        top_inv = []
    t_inv = time.time() - start

    # --- FAISS HNSW ---
    start = time.time()
    faiss_index = faiss.IndexHNSWFlat(query_vec.shape[1], 32)
    faiss_index.hnsw.efConstruction = 40
    faiss_index.add(tfidf_subset)
    D, I = faiss_index.search(query_vec, 5)
    top_faiss = [(D[0][j], id_subset[I[0][j]]) for j in range(5)]
    t_faiss = time.time() - start

    print(f"✅ N={N:5d} | Sec: {t_seq:.3f}s | Inv: {t_inv:.3f}s | FAISS: {t_faiss:.3f}s")
