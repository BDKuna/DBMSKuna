import csv
import os
import pickle
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from indexes.invertedindex import BUCKET_LIMIT, BType, DocumentFile, InvertedFile
from preprocessing.text_utils import bagOfWords


class TextIndexer:
    """Utility to build an inverted index from a CSV file."""

    def __init__(self, index_path: str, doc_path: str) -> None:
        self.inverted = InvertedFile(index_path)
        self.docs = DocumentFile(doc_path)
        self._bucket: BType = {}

    def _flush(self) -> None:
        if self._bucket:
            self.inverted.append(self._bucket)
            self._bucket = {}

    def add_document(self, doc_id: str, text: str) -> None:
        bow = bagOfWords(text)
        self.docs.append(doc_id, len(bow))

        pending_terms = list(bow.items())
        idx = 0
        while idx < len(pending_terms):
            word, freq = pending_terms[idx]
            self._bucket.setdefault(word, {})[doc_id] = freq

            if len(pickle.dumps(self._bucket)) > BUCKET_LIMIT:
                # revert and flush
                self._bucket[word].pop(doc_id, None)
                if not self._bucket[word]:
                    del self._bucket[word]
                self._flush()
            else:
                idx += 1

    def finalize(self) -> None:
        self._flush()


def processingDatasetOnInvertedFile(csv_path: str, column: str) -> str:
    """Process a CSV and create inverted index files."""

    index_path = csv_path[:-4] + "_inv.dat"
    doc_path = csv_path[:-4] + "_doc.dat"

    if os.path.exists(index_path):
        os.remove(index_path)
    if os.path.exists(doc_path):
        os.remove(doc_path)

    indexer = TextIndexer(index_path, doc_path)

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            if idx % 1000 == 0:
                print(f"Processing {idx} text")
            #if idx == 3000 : break
            doc_id = f"t-{idx}"
            indexer.add_document(doc_id, row[column])

    indexer.finalize()
    return index_path
