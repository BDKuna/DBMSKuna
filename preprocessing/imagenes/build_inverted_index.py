import os
import numpy as np
from tqdm import tqdm
from indexes.visualindex import VisualInvertedIndex

# --- Configuración ---
K = 100  # Número de visual words (ajusta según modelo)
ROOT = "F:/fashion_dataset/archive/fashion-dataset"
HIST_FOLDER = os.path.join(ROOT, f"histograms_k{K}")
OUT_PATH = os.path.join(ROOT, f"visual_index_k{K}.pkl")

# --- Inicializar índice invertido visual ---
index = VisualInvertedIndex()

print(f"📂 Cargando histogramas desde: {HIST_FOLDER}")
files = [f for f in os.listdir(HIST_FOLDER) if f.endswith(".npy")]

for fname in tqdm(files):
    path = os.path.join(HIST_FOLDER, fname)
    hist = np.load(path)
    if hist is None or len(hist) == 0:
        continue

    img_id = int(os.path.splitext(fname)[0])

    for word_id, freq in enumerate(hist):
        if freq > 0:
            index.insert(word_id, img_id, int(freq))

# --- Guardar índice ---
print(f"💾 Guardando índice invertido en: {OUT_PATH}")
index.save(OUT_PATH)
print("✅ Índice invertido visual generado correctamente.")
