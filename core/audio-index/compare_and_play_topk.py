# Que alguien haga algo interactivo para el proyecto yo hice esto.
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pickle
import numpy as np
import faiss
import heapq
from sklearn.metrics.pairwise import cosine_similarity
from playsound import playsound

from preprocessing.audio.audio_utils import (
    extract_mfcc_from_wav, build_histogram_from_mfcc, tfidf_transform, load_centroids
)
from indexes.audioindex import AudioInvertedIndex

# --- Configuración ---
ROOT = "F:/spotify_songs/dataset"
K = 100
QUERY_PATH = os.path.join(ROOT, "The-Weeknd-Blinding-Lights-.wav")
TFIDF_PATH = os.path.join(ROOT, f"tfidf_audio_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"audio_index_k{K}.pkl")
TOP_K = 5

# --- Cargar TF-IDF y codebook ---
with open(TFIDF_PATH, "rb") as f:
    tfidf_dict = pickle.load(f)
centroids = load_centroids()
audio_ids = sorted(tfidf_dict.keys())
tfidf_matrix = np.stack([tfidf_dict[a] for a in audio_ids]).astype("float32")
idf = np.log((1 + len(tfidf_dict)) / (1 + (np.array([v > 0 for v in tfidf_dict.values()]).sum(axis=0)))) + 1

# --- Extraer TF-IDF de la query ---
mfcc = extract_mfcc_from_wav(QUERY_PATH)
hist = build_histogram_from_mfcc(mfcc, centroids)
query_vec = tfidf_transform(hist, idf).reshape(1, -1).astype("float32")

# --- Secuencial ---
sec_scores = []
for i, audio_id in enumerate(audio_ids):
    sim = cosine_similarity(query_vec, tfidf_matrix[i].reshape(1, -1))[0][0]
    sec_scores.append((sim, audio_id))
top_sec = [a for _, a in heapq.nlargest(TOP_K, sec_scores)]

# --- Invertido ---
index = AudioInvertedIndex.load(INDEX_PATH)
nonzero_words = np.nonzero(hist)[0]
candidates = list(index.get_candidates(nonzero_words))
inv_scores = []
for audio_id in candidates:
    vec = tfidf_dict.get(audio_id)
    if vec is not None:
        sim = cosine_similarity(query_vec, vec.reshape(1, -1))[0][0]
        inv_scores.append((sim, audio_id))
top_inv = [a for _, a in heapq.nlargest(TOP_K, inv_scores)]

# --- FAISS ---
faiss_index = faiss.IndexHNSWFlat(query_vec.shape[1], 32)
faiss_index.add(tfidf_matrix)
D, I = faiss_index.search(query_vec, TOP_K)
top_faiss = [audio_ids[i] for i in I[0]]

# --- Combinar resultados ---
all_ids = top_sec + top_inv + top_faiss
votes = {}
for audio_id in all_ids:
    votes[audio_id] = votes.get(audio_id, 0) + 1

# Ordenar por coincidencias (votos)
ranked = sorted(votes.items(), key=lambda x: (-x[1], x[0]))

# --- Reproducir ---
print("\n🔊 Reproduciendo resultados por coincidencia entre métodos:\n")
for i, (audio_id, score) in enumerate(ranked):
    subdir = str(audio_id).zfill(6)[:3]
    wav_path = os.path.join(ROOT, "fma_small", subdir, f"{audio_id}.mp3")

    if os.path.exists(wav_path):
        print(f"{i+1}. [{score} coincidencias] -> {audio_id}")
        playsound(wav_path)
        input("▶ Presiona Enter para reproducir el siguiente...")
    else:
        print(f"⚠️ Archivo no encontrado: {wav_path}")


