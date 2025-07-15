import os
import numpy as np
import pickle
from tqdm import tqdm

# Configuración
ROOT = "F:/spotify_songs/dataset"
DESC_FOLDER = os.path.join(ROOT, "mfcc_descriptors")
OUT_PATH = os.path.join(ROOT, "mfcc_index.pkl")

index = {}

print(f"📂 Cargando MFCCs desde: {DESC_FOLDER}")
for fname in tqdm(os.listdir(DESC_FOLDER)):
    if not fname.endswith(".npy"):
        continue
    audio_id = os.path.splitext(fname)[0]
    fpath = os.path.join(DESC_FOLDER, fname)
    mfcc = np.load(fpath)
    if mfcc is not None and len(mfcc) > 0:
        index[audio_id] = mfcc

# Guardar índice
with open(OUT_PATH, "wb") as f:
    pickle.dump(index, f)

print(f"✅ Índice de MFCCs guardado en: {OUT_PATH} | Total: {len(index)} audios")
