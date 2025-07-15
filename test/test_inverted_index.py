import os
import csv
import sys

# Ensure project root is in path for relative imports
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ROOT not in sys.path:
    sys.path.append(ROOT)

from preprocessing.text import processingDatasetOnInvertedFile
from indexes.invertedindex import InvertedIndex

CSV_PATH = os.path.normpath(os.path.join(ROOT, 'datasets', 'data2', 'mpst_full_data.csv'))
INDEX_PATH = os.path.normpath(os.path.join(ROOT, 'datasets', 'data2', 'mpst_full_data_inv.dat'))


def _prepare_index() -> InvertedIndex:
    """Create the inverted index if necessary and build it."""
    if not os.path.exists(INDEX_PATH):
        processingDatasetOnInvertedFile(CSV_PATH, 'plot_synopsis')
        index = InvertedIndex(INDEX_PATH)
        index.buildIndex()
    index = InvertedIndex(INDEX_PATH)
    return index


def test_search_returns_results():
    index = _prepare_index()
    results = index.searchQuery('jurassic park', 5)
    assert results, 'Expected at least one result for query'


if __name__ == '__main__':
    idx = _prepare_index()
    res = idx.searchQuery('jurassic park park', 5)
    # Sort results by document id so they match the order used when
    # printing the document contents below.
    res.sort(key=lambda x: x[1], reverse=True)
    print('Documentos encontrados:', res)
    ids = set(int(doc_id[2:]) for doc_id, _ in res)
    with open(CSV_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i in ids:
                print(f"\n🗂️ Documento ID: t-{i}")
                print(f"📄 Nombre:\n{row['title']}")
                print(f"📄 Texto:\n{row['plot_synopsis']}")
