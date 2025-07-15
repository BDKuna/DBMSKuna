import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import time
import pickle
import numpy as np
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity

start = time.time()

# --- Configuración ---
ROOT = "F:/fashion_dataset/archive/fashion-dataset"
K = 1000
TFIDF_PATH = os.path.join(ROOT, f"tfidf_k{K}.pkl")
QUERY_ID = "10001"  # imagen query

# --- Cargar vectores TF-IDF ---
print("📂 Cargando vectores TF-IDF...")
with open(TFIDF_PATH, "rb") as f:
    tfidf_vectors = pickle.load(f)

assert QUERY_ID in tfidf_vectors, f"La imagen {QUERY_ID} no se encuentra en los vectores TF-IDF"

query_vec = tfidf_vectors[QUERY_ID].reshape(1, -1)

# --- Comparar contra todos ---
print("🔍 Comparando con todos los vectores...")
results = []
for img_id, vec in tqdm(tfidf_vectors.items()):
    if img_id == QUERY_ID:
        continue
    sim = cosine_similarity(query_vec, vec.reshape(1, -1))[0][0]
    results.append((sim, img_id))

# --- Mostrar resultados ---
results.sort(reverse=True)
print("\n📌 Resultados más similares:")
for sim, img_id in results[:5]:
    print(f"🔸 {img_id}  | Score: {sim:.4f}")

print(f"⏱ Tiempo total: {time.time() - start:.2f} segundos")
