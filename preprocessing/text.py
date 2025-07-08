import os
import sys
import re
import string
import pickle
import csv
from typing import Dict, List, Optional, Iterator, Tuple, OrderedDict


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


from indexes.invertedindex import InvertedFile, BUCKET_LIMIT, DocumentFile, BType
from preprocessing.text_utils import bagOfWords

# --- Configuración fija ---

def processingDatasetOnInvertedFile(csv_path : str, column : str) -> str:
    """
    1. Lee CSV con columnas: title,text,subject,date
    2. Para cada fila, usa `subject` y genera ID como t-0, t-1, ...
    3. Genera bag of words
    4. Inserta en bucket hasta que pase el límite, entonces hace flush parcial
    5. Al final, flush final
    """

    index_path = csv_path[:-4] + "_inv.dat"
    doc_path = csv_path[:-4] + "_doc.dat"

    if os.path.exists(index_path):
        os.remove(index_path)
    if os.path.exists(doc_path):
        os.remove(doc_path)

    inv = InvertedFile(index_path)
    doc = DocumentFile(doc_path)
    bucket: BType = {}

    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if idx % 1000 == 0:
                print(f"Processing {idx} text")
            if idx == 3000 : break
            doc_id = f"t-{idx}"
            full_bow = bagOfWords(row[column])
            doc.append(doc_id, len(full_bow.keys()))

            # Lista de palabras por insertar
            pending_terms = list(full_bow.items())
            term_idx = 0

            while term_idx < len(pending_terms):
                word, freq = pending_terms[term_idx]
                # Insertar la palabra en el bucket
                bucket.setdefault(word, {})[doc_id] = freq

                # Verificar si excede el límite
                raw = pickle.dumps(bucket)
                if len(raw) > BUCKET_LIMIT:
                    # Deshacer inserción de esta palabra
                    bucket[word].pop(doc_id, None)
                    if not bucket[word]:
                        del bucket[word]

                    inv.append(bucket)

                    # Reiniciar bucket
                    bucket = {}
                    # No aumentar term_idx, este término no fue insertado aún
                else:
                    term_idx += 1  # Esta palabra sí se insertó, pasar a la siguiente

    # Flush final
    if bucket:
        inv.append(bucket)
    
    return index_path
