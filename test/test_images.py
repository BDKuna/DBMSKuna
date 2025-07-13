import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from preprocessing.image_utils import verificar_dimensiones, asociar_con_metadatos

IMG_PATH = "F:/fashion_dataset/archive/fashion-dataset/images"
CSV_PATH = "F:/fashion_dataset/archive/fashion-dataset/styles.csv"
PKL_OUT = "F:/fashion_dataset/archive/fashion-dataset/valid_metadata.pkl"

verificar_dimensiones(IMG_PATH)
asociar_con_metadatos(CSV_PATH, IMG_PATH, guardar_en=PKL_OUT)
