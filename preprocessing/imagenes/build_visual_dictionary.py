import os
import numpy as np
from tqdm import tqdm

# Rutas
ROOT_FOLDER = "F:/fashion_dataset/archive/fashion-dataset"
DESCRIPTOR_FOLDER = os.path.join(ROOT_FOLDER, "descriptors")
OUTPUT_FILE = os.path.join(ROOT_FOLDER, "all_descriptors.npy")

# Configuración (opcional: limitar por memoria)
MAX_DESCRIPTORS = 1_000_000  # Ajusta según tu RAM (1 millón ≈ 500 MB)

# Recolectar todos los archivos .npy
files = [f for f in os.listdir(DESCRIPTOR_FOLDER) if f.endswith(".npy")]
print(f"📦 Archivos de descriptores encontrados: {len(files)}")

all_desc = []
total = 0

# Leer y acumular descriptores
for f in tqdm(files):
    path = os.path.join(DESCRIPTOR_FOLDER, f)
    desc = np.load(path)

    if desc is not None and len(desc) > 0:
        all_desc.append(desc)
        total += len(desc)

    # Detener si se excede el máximo
    if total >= MAX_DESCRIPTORS:
        break

# Concatenar en una sola matriz
final_matrix = np.vstack(all_desc)
print(f"✅ Total de descriptores unidos: {final_matrix.shape}")

# Guardar para usar en KMeans
np.save(OUTPUT_FILE, final_matrix)
print(f"💾 Guardado en: {OUTPUT_FILE}")
