import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import time
import pickle
import numpy as np
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
from indexes.visualindex import VisualInvertedIndex

start = time.time()

# --- Configuración ---
ROOT = "F:/fashion_dataset/archive/fashion-dataset"
K = 1000
TFIDF_PATH = os.path.join(ROOT, f"tfidf_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"visual_index_k{K}.pkl")
QUERY_ID = "10001"

# --- Cargar vectores TF-IDF y ordenarlos ---
print("📂 Cargando vectores TF-IDF...")
with open(TFIDF_PATH, "rb") as f:
    tfidf_dict = pickle.load(f)

img_ids = sorted(tfidf_dict.keys())
img_idx_map = {img_id: idx for idx, img_id in enumerate(img_ids)}
tfidf_matrix = np.stack([tfidf_dict[img_id] for img_id in img_ids])

# --- Cargar índice invertido ---
print("📂 Cargando índice invertido...")
index = VisualInvertedIndex.load(INDEX_PATH)

# --- Preparar query ---
assert QUERY_ID in tfidf_dict, f"{QUERY_ID} no encontrado en TF-IDF"
query_vec = tfidf_dict[QUERY_ID].reshape(1, -1)
nonzero_words = np.nonzero(query_vec)[1]

# --- Obtener candidatos únicos y ordenados ---
candidates_set = index.get_candidates(nonzero_words)
candidates = sorted(candidates_set)
print(f"🔍 Comparando contra {len(candidates)} candidatos")

# --- Vectorizar búsqueda ---
candidates_vectors = []
candidates_ids = []

for img_id in candidates:
    if str(img_id) == QUERY_ID:
        continue  # Evita self-match
    vec = tfidf_dict.get(str(img_id))
    if vec is not None:
        candidates_ids.append(img_id)
        candidates_vectors.append(vec)

candidates_matrix = np.stack(candidates_vectors)
similarities = cosine_similarity(query_vec, candidates_matrix)[0]

# --- Mostrar resultados ---
top_indices = similarities.argsort()[::-1][:5]
print("\n📌 Resultados más similares:")
for i in top_indices:
    print(f"🔸 {candidates_ids[i]}  | Score: {similarities[i]:.4f}")

print(f"⏱ Tiempo total: {time.time() - start:.2f} segundos")
