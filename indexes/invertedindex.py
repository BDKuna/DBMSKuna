import struct
import os
import sys
import pickle
from typing import Dict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from preprocessing.text import *
import math

import logger

BUCKET_LIMIT = 1024

class InvertedFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger(f"INVERTED-FILE-{filename}".upper())

        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                self.logger.info(f"Archivo {filename} creado.")

    def _serialize(self, d: Dict) -> bytes:
        data = pickle.dumps(d)
        if len(data) > BUCKET_LIMIT:
            raise ValueError(f"El diccionario serializado excede BUCKET_LIMIT de {BUCKET_LIMIT} bytes.")
        return data.ljust(BUCKET_LIMIT, b'\x00')

    def _deserialize(self, b: bytes) -> Dict:
        try:
            return pickle.loads(b.rstrip(b'\x00'))
        except Exception as e:
            self.logger.error(f"Error de deserialización: {e}")
            return {}

    def read(self, pos: int) -> Dict:
        with open(self.filename, 'rb') as f:
            f.seek(pos * BUCKET_LIMIT)
            data = f.read(BUCKET_LIMIT)
            if not data:
                self.logger.warning(f"Intento de lectura en posición vacía: {pos}")
                return {}
            return self._deserialize(data)

    def write(self, pos: int, d: Dict):
        data = self._serialize(d)
        with open(self.filename, 'r+b') as f:
            f.seek(pos * BUCKET_LIMIT)
            f.write(data)
        self.logger.info(f"Escrito bucket en posición {pos}.")

    def append(self, d: Dict) -> int:
        data = self._serialize(d)
        with open(self.filename, 'ab') as f:
            f.write(data)
        pos = os.path.getsize(self.filename) // BUCKET_LIMIT - 1
        self.logger.info(f"Append en posición {pos}.")
        return pos

def avg_entry_size(d: dict) -> int:
    return len(pickle.dumps(d)) // max(1, len(d))

class InvertedIndex:
    def __init__(self):
        # Archivo de postings
        self.postings = InvertedFile("postings.dat")
        # Archivo que guarda en cada bucket un dict {'pos':…, 'len':…}
        self.texts     = InvertedFile("table_column_texts.dat")
        self.raw_filename = "data.txt"  # <-- nombre de tu archivo con los textos crudos
        self.norms     = {}
        # contamos cuántos metadatos hay
        self.total_docs = 0
        while True:
            meta = self.texts.read(self.total_docs)
            if not meta:
                break
            self.total_docs += 1

    def _load_text(self, pos: int, length: int) -> str:
        """Lee `length` bytes desde el archivo raw_filename en la posición `pos`."""
        with open(self.raw_filename, "rb") as f:
            f.seek(pos)
            return f.read(length).decode("utf-8")

    def buildIndex(self):
        partial = {}
        term_count = 0
        blocks = []

        # recorremos todos los metadatos (doc_id == índice de bucket)
        for doc_id in range(self.total_docs):
            meta = self.texts.read(doc_id)        # {'pos': X, 'len': Y}
            raw = self._load_text(meta['pos'], meta['len'])
            bow = bagOfWords(raw)

            for term, tf in bow.items():
                partial.setdefault(term, {})[doc_id] = tf
                term_count += 1

                if term_count * avg_entry_size(partial) >= BUCKET_LIMIT:
                    blk_name = f"spimi_{len(blocks)}.blk"
                    blk = InvertedFile(blk_name)
                    for t in sorted(partial):
                        blk.append({t: partial[t]})
                    blocks.append(blk_name)
                    partial.clear()
                    term_count = 0

        # flush final
        if partial:
            blk_name = f"spimi_{len(blocks)}.blk"
            blk = InvertedFile(blk_name)
            for t in sorted(partial):
                blk.append({t: partial[t]})
            blocks.append(blk_name)

        # merge hasta un solo bloque
        while len(blocks) > 1:
            f1, f2 = blocks.pop(0), blocks.pop(0)
            out = f"spimi_{len(blocks)}.blk"
            self._merge_files(f1, f2, out)
            blocks.append(out)

        # movemos bloque final a postings.dat
        os.replace(blocks[0], self.postings.filename)

        # calculamos normas definitivas
        for doc_id in range(self.total_docs):
            meta = self.texts.read(doc_id)
            raw = self._load_text(meta['pos'], meta['len'])
            bow = bagOfWords(raw)
            norm2 = 0.0
            for term, tf in bow.items():
                plist, _ = self.getByWord(term)
                idf = math.log(self.total_docs / max(1, len(plist)))
                norm2 += (tf * idf) ** 2
            self.norms[doc_id] = math.sqrt(norm2)

    def _merge_files(self, f1: str, f2: str, out: str):
        A, B, O = InvertedFile(f1), InvertedFile(f2), InvertedFile(out)
        pos1 = pos2 = 0
        buf1 = A.read(pos1)
        buf2 = B.read(pos2)
        out_buf = {}

        while buf1 or buf2:
            t1 = next(iter(buf1)) if buf1 else None
            t2 = next(iter(buf2)) if buf2 else None

            if t2 is None or (t1 is not None and t1 < t2):
                out_buf[t1] = buf1.pop(t1)
            elif t1 is None or (t2 is not None and t2 < t1):
                out_buf[t2] = buf2.pop(t2)
            else:
                p1 = buf1.pop(t1)
                p2 = buf2.pop(t2)
                merged = p1.copy()
                for d, tf in p2.items():
                    merged[d] = merged.get(d, 0) + tf
                out_buf[t1] = merged

            if len(out_buf) * avg_entry_size(out_buf) >= BUCKET_LIMIT:
                O.append(out_buf)
                out_buf.clear()

            if not buf1:
                pos1 += 1
                buf1 = A.read(pos1)
            if not buf2:
                pos2 += 1
                buf2 = B.read(pos2)

        if out_buf:
            O.append(out_buf)


    def getByWord(self,word)->(dict[int,int],int):
        # se necesita devolver los documentos con su tf, y el idf, en el q se encuntra la palabra
        index = {1:1,2:2,3:1,4:4,5:3}
        total_docs = 100 #guardar en header
        idf = math.log(total_docs / len(index))
        return index, idf

    def getLengthDoc(self,doc_id):
        #returns the lenght of the document (plis)
        return 1000


    def search(self, consulta:str)->list[int]:
        query_tf = bagOfWords(consulta)
        vector_doc = [self.getByWord(word) for word in query_tf]

        # Calcular TF-IDF de la consulta
        query_tf_idf = {word: (tf * vector_doc[i][1]) for i,(word, tf) in enumerate(query_tf.items())}
        query_norm = math.sqrt((sum(value ** 2 for value in query_tf_idf.values())))

        score = {}

        # Calcular similitud de cosenos para cada documento
        for i, word in enumerate(query_tf):
            for doc_id, tf in vector_doc[i][0].items():
                if doc_id not in score:
                    score[doc_id] = 0
                # Calcular TF-IDF del documento
                doc_tf_idf = tf * vector_doc[i][1]
                score[doc_id] += query_tf_idf[word] * doc_tf_idf # Producto punto

        # Normalizar por la longitud de los vectores para la definición de similitud de cosenos
        for doc_id in score:
            score[doc_id] /= (query_norm * self.getLengthDoc(doc_id))

        # Ordenar los resultados por similitud
        result = sorted(score.items(), key=lambda tup: tup[1], reverse=True)
        return result

if __name__ == "__main__":

    # 1) Lee t odo el contenido crudo
    with open("data.txt", "rb") as f:
        raw = f.read()

    # 2) Crea el archivo de metadatos
    meta = InvertedFile("table_column_texts.dat")
    offset = 0
    for line in raw.split(b"\n"):
        length = len(line)
        meta.append({'pos': offset, 'len': length})
        offset += length + 1  # +1 por el salto de línea

    idx = InvertedIndex()
    idx.buildIndex()
    for q in ["hola", "mundo", "hola mundo", "tardes"]:
        print(f">> {q} →", idx.search(q))