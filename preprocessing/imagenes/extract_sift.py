import os
import cv2
import pickle
import numpy as np
from tqdm import tqdm

# Rutas adaptadas a tu estructura actual
ROOT_FOLDER = "F:/fashion_dataset/archive/fashion-dataset"
IMG_FOLDER = os.path.join(ROOT_FOLDER, "images")
DESCRIPTOR_FOLDER = os.path.join(ROOT_FOLDER, "descriptors")
METADATA_PATH = os.path.join(ROOT_FOLDER, "valid_metadata.pkl")

# Asegurarse de que exista carpeta de descriptores
os.makedirs(DESCRIPTOR_FOLDER, exist_ok=True)

# Cargar metadatos válidos
with open(METADATA_PATH, "rb") as f:
    df = pickle.load(f)

# Inicializar extractor SIFT
sift = cv2.SIFT_create()

# Contadores
guardados = 0
omitidos = 0

print("🔍 Extrayendo descriptores SIFT...")

for _, row in tqdm(df.iterrows(), total=len(df)):
    img_id = row["id"]
    img_path = os.path.join(IMG_FOLDER, f"{img_id}.jpg")
    out_path = os.path.join(DESCRIPTOR_FOLDER, f"{img_id}.npy")

    # Saltar si ya existe el descriptor
    if os.path.exists(out_path):
        continue

    img = cv2.imread(img_path, cv2.IMREAD_GRAYSCALE)
    if img is None:
        print(f"⚠️ Imagen no encontrada o corrupta: {img_id}")
        omitidos += 1
        continue

    # Redimensionar a tamaño fijo
    img_resized = cv2.resize(img, (224, 224))

    keypoints, descriptors = sift.detectAndCompute(img_resized, None)

    if descriptors is not None and len(descriptors) > 0:
        np.save(out_path, descriptors)
        guardados += 1
    else:
        omitidos += 1

print(f"\n✅ Finalizado. Guardados: {guardados}  |  Omitidos (vacíos o corruptos): {omitidos}")
