import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
import pygame
import cv2
import time
import pickle
import numpy as np
from tkinter import Tk, filedialog
from sklearn.metrics.pairwise import cosine_similarity
from indexes.visualindex import VisualInvertedIndex

# --- Configuración ---
ROOT = "../../datasets/fashion-dataset"
IMAGES_PATH = os.path.join(ROOT, "images")
TFIDF_PATH = os.path.join(ROOT, "tfidf_k100.pkl")
INDEX_PATH = os.path.join(ROOT, "visual_index_k100.pkl")
K = 100
NUM_RESULTS = 3
IMG_SIZE = (150, 150)

# --- Cargar TF-IDF e índice invertido ---
with open(TFIDF_PATH, "rb") as f:
    tfidf_dict = pickle.load(f)

index = VisualInvertedIndex.load(INDEX_PATH)

# --- Funciones de apoyo ---
def cargar_imagen(path):
    img = cv2.imread(path)
    if img is None:
        return None
    img = cv2.resize(img, IMG_SIZE)
    img = cv2.cvtColor(img, cv2.COLOR_BGR2RGB)
    return pygame.surfarray.make_surface(np.transpose(img, (1, 0, 2)))

def seleccionar_imagen():
    Tk().withdraw()
    filepath = filedialog.askopenfilename(title="Selecciona una imagen .jpg", filetypes=[("Imagen JPG", "*.jpg")])
    return filepath

def calcular_similares(query_path):
    img_id = os.path.splitext(os.path.basename(query_path))[0]
    if img_id not in tfidf_dict:
        print("❌ Imagen no encontrada en el índice.")
        return [], 0.0

    query_vec = tfidf_dict[img_id].reshape(1, -1)
    nonzero_words = np.nonzero(query_vec)[1]
    candidates = index.get_candidates(nonzero_words)

    results = []
    start = time.time()

    for cand_id in candidates:
        cand_id = str(cand_id)
        if cand_id == img_id or cand_id not in tfidf_dict:
            continue
        vec = tfidf_dict[cand_id]
        sim = cosine_similarity(query_vec, vec.reshape(1, -1))[0][0]
        results.append((sim, cand_id))

    results.sort(reverse=True)
    elapsed = time.time() - start
    return results[:NUM_RESULTS], elapsed

# --- Inicializar Pygame ---
pygame.init()
WIDTH, HEIGHT = 1000, 500
screen = pygame.display.set_mode((WIDTH, HEIGHT))
pygame.display.set_caption("Visual Similarity Search")
font = pygame.font.SysFont("arial", 18)

def main():
    running = True
    query_img = None
    result_imgs = []
    scores = []
    tiempo = 0.0

    while running:
        screen.fill((30, 30, 30))

        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                running = False

            elif event.type == pygame.KEYDOWN and event.key == pygame.K_SPACE:
                path = seleccionar_imagen()
                if path:
                    query_img = cargar_imagen(path)
                    resultados, tiempo = calcular_similares(path)
                    result_imgs = [cargar_imagen(os.path.join(IMAGES_PATH, f"{id_}.jpg")) for _, id_ in resultados]
                    scores = [sim for sim, _ in resultados]

        # Mostrar query
        if query_img:
            screen.blit(query_img, (50, 50))
            screen.blit(font.render("Query", True, (255, 255, 255)), (50, 30))

        # Mostrar resultados
        for i, (img, score) in enumerate(zip(result_imgs, scores)):
            if img:
                x = 250 + i * (IMG_SIZE[0] + 20)
                screen.blit(img, (x, 50))
                screen.blit(font.render(f"Score: {score:.3f}", True, (200, 200, 200)), (x, 220))

        # Tiempo
        if query_img:
            screen.blit(font.render(f"Tiempo de búsqueda: {tiempo:.2f} s", True, (255, 255, 0)), (50, 400))
            screen.blit(font.render("Presiona [ESPACIO] para seleccionar imagen", True, (180, 180, 180)), (50, 430))

        pygame.display.flip()

    pygame.quit()

if __name__ == "__main__":
    main()