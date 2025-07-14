import os
import sys
import csv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from preprocessing.text import processingDatasetOnInvertedFile
from indexes.invertedindex import InvertedIndex

CSV_PATH = os.path.join(os.path.dirname(__file__), "..", "datasets", "data2", "mpst_full_data.csv")

"""
if __name__ == "__main__":
    index_filename = processingDatasetOnInvertedFile(CSV_PATH, "plot_synopsis")
    index : InvertedIndex = InvertedIndex(index_filename)
    index.buildIndex()
"""

INDEX_PATH = os.path.join(os.path.dirname(__file__), "..", "datasets", "data2", "mpst_full_data_inv.dat")

if __name__ == "__main__":
    index = InvertedIndex(INDEX_PATH)

    results = index.searchQuery("world wide web", 5)
    print("Documentos encontrados:", results)

    # Extraer las líneas que corresponden a los resultados
    result_ids = set(int(doc_id[2:]) for doc_id, _ in results)

    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if idx in result_ids:
                print(f"\n🗂️ Documento ID: t-{idx}")
                print(f"📄 Nombre:\n{row['title']}")
                print(f"📄 Texto:\n{row['plot_synopsis']}")

