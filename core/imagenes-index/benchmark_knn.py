import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pickle
import time
import numpy as np
from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
import faiss
from indexes.visualindex import VisualInvertedIndex

# --- Configuración ---
ROOT = "F:/fashion_dataset/archive/fashion-dataset"
K = 100
TFIDF_PATH = os.path.join(ROOT, f"tfidf_k{K}.pkl")
INDEX_PATH = os.path.join(ROOT, f"visual_index_k{K}.pkl")
QUERY_ID = "10001"
RESULTS_FILE = os.path.join(ROOT, "benchmark_results.csv")

# --- Cargar vectores TF-IDF completos ---
print("📂 Cargando todos los vectores TF-IDF...")
with open(TFIDF_PATH, "rb") as f:
    full_tfidf = pickle.load(f)

assert QUERY_ID in full_tfidf, f"Query ID {QUERY_ID} no encontrado"

# --- Cargar índice invertido completo ---
print("📂 Cargando índice invertido completo...")
full_index = VisualInvertedIndex.load(INDEX_PATH)

# --- Preparar valores de N ---
sizes = [1000, 2000, 4000, 8000, 16000, 32000, len(full_tfidf)]

# --- Función: búsqueda secuencial ---
def search_sequential(query_vec, tfidf_subset):
    sims = []
    for img_id, vec in tfidf_subset.items():
        if img_id == QUERY_ID:
            continue
        sim = cosine_similarity(query_vec, vec.reshape(1, -1))[0][0]
        sims.append(sim)
    return sims

# --- Función: búsqueda con índice invertido optimizado ---
def search_inverted(query_vec, tfidf_subset, index):
    nonzero_words = np.nonzero(query_vec)[1]
    candidates = index.get_candidates(nonzero_words)
    candidates_vectors = []
    candidates_ids = []
    for img_id in candidates:
        img_id_str = str(img_id)
        if img_id_str == QUERY_ID:
            continue
        vec = tfidf_subset.get(img_id_str)
        if vec is not None:
            candidates_ids.append(img_id_str)
            candidates_vectors.append(vec)
    if not candidates_vectors:
        return []
    candidates_matrix = np.stack(candidates_vectors)
    similarities = cosine_similarity(query_vec, candidates_matrix)[0]
    return similarities

# --- Función: búsqueda con FAISS-HNSW ---
def search_hnsw(query_vec, tfidf_subset):
    ids = list(tfidf_subset.keys())
    vectors = np.stack([tfidf_subset[i] for i in ids]).astype('float32')
    index = faiss.IndexHNSWFlat(vectors.shape[1], 32)
    index.hnsw.efSearch = 64
    index.hnsw.efConstruction = 40
    index.add(vectors)
    _, _ = index.search(query_vec.astype('float32'), 8)
    return

# --- Archivo de resultados ---
with open(RESULTS_FILE, "w") as f:
    f.write("N,secuencial,invertido,hnsw\\n")

# --- Benchmark principal ---
for N in sizes:
    print(f"⚙️ Probando con N = {N}")
    tfidf_subset = dict(list(full_tfidf.items())[:N])
    query_vec = tfidf_subset[QUERY_ID].reshape(1, -1)

    # Secuencial
    t0 = time.time()
    search_sequential(query_vec, tfidf_subset)
    t1 = time.time()

    # Invertido (filtrando índice y aplicando vectorización)
    inv_index = VisualInvertedIndex()
    for word_id in full_index.index:
        for img_id, freq in full_index.index[word_id].items():
            if str(img_id) in tfidf_subset:
                inv_index.insert(word_id, img_id, freq)

    t2 = time.time()
    search_inverted(query_vec, tfidf_subset, inv_index)
    t3 = time.time()

    # FAISS-HNSW
    t4 = time.time()
    search_hnsw(query_vec, tfidf_subset)
    t5 = time.time()

    t_seq = round(t1 - t0, 3)
    t_inv = round(t3 - t2, 3)
    t_hnsw = round(t5 - t4, 3)

    print(f"✅ N={N} | Sec: {t_seq}s | Inv: {t_inv}s | HNSW: {t_hnsw}s")

    with open(RESULTS_FILE, "a") as f:
        f.write(f"{N},{t_seq},{t_inv},{t_hnsw}\\n")
