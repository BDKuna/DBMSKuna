import os
import sys
import pickle
import struct
from typing import Dict, List, Optional, Iterator, Tuple, OrderedDict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from preprocessing.text import *
import math

import logger

BUCKET_LIMIT = 1024

BType = Dict[str, Dict[str, int]]

class InvertedFile:
    HEADER_FORMAT = "i"
    HEADER_SIZE = 4
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger(f"INVERTED-FILE-{filename}".upper())
        import logging
        self.logger.logger.setLevel(logging.INFO)
        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                header = struct.pack(self.HEADER_FORMAT, 0)
                f.write(header)
            self.logger.info(f"Archivo {filename} creado con header estructurado.")

    def _serialize(self, d: BType) -> bytes:
        data = pickle.dumps(d)
        if len(data) > BUCKET_LIMIT:
            raise ValueError(f"El diccionario serializado excede BUCKET_LIMIT de {BUCKET_LIMIT} bytes.")
        return data.ljust(BUCKET_LIMIT, b'\x00')

    def _deserialize(self, b: bytes) -> BType:
        try:
            return pickle.loads(b.rstrip(b'\x00'))
        except Exception as e:
            self.logger.error(f"Error de deserialización: {e}")
            return {}

    def _write_header(self, num_buckets: int):
        packed = struct.pack(self.HEADER_FORMAT, num_buckets)
        with open(self.filename, 'r+b') as f:
            f.seek(0)
            f.write(packed)

    def _read_header(self) -> int:
        with open(self.filename, 'rb') as f:
            f.seek(0)
            data = f.read(struct.calcsize(self.HEADER_FORMAT))
            if len(data) < 4:
                return 0
            return struct.unpack(self.HEADER_FORMAT, data)[0]

    def read(self, pos: int) -> BType:
        with open(self.filename, 'rb') as f:
            f.seek(self.HEADER_SIZE + pos * BUCKET_LIMIT)
            data = f.read(BUCKET_LIMIT)
            if not data:
                self.logger.warning(f"Intento de lectura en posición vacía: {pos}")
                return {}
            return self._deserialize(data)

    def write(self, pos: int, d: BType):
        data = self._serialize(d)
        with open(self.filename, 'r+b') as f:
            f.seek(self.HEADER_SIZE + pos * BUCKET_LIMIT)
            f.write(data)
        self.logger.debug(f"Escrito bucket en posición {pos}.")

    def append(self, d: BType) -> int:
        num_buckets = self._read_header()
        data = self._serialize(d)

        with open(self.filename, 'r+b') as f:
            f.seek(self.HEADER_SIZE + num_buckets * BUCKET_LIMIT)
            f.write(data)

        self._write_header(num_buckets + 1)
        self.logger.debug(f"Append en posición {num_buckets}.")
        return num_buckets
    
    def show(self):
        num_buckets = self._read_header()
        self.logger.info(f"Mostrando contenido de {self.filename} ({num_buckets} buckets):")

        for i in range(num_buckets):
            bucket = self.read(i)
            print(f"Bucket {i}: {bucket}")
            print(len(pickle.dumps(bucket)))


class InvertedIndex:
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger("INVERTED-INDEX")

        if not os.path.exists(filename):
            self.logger.error(f"Archivo {filename} no existe.")
            raise FileNotFoundError(f"Archivo {filename} no encontrado.")

        self.logger.info(f"Inicializando InvertedIndex con archivo {filename}.")
        self.file = InvertedFile(filename)

    def insert_buckets(self, buckets : list[BType]):
        for b in buckets:
            self.file.append(b)
    
    def _sort_dict(self, d: Dict) -> Dict:
        return {
            word: dict(sorted(postings.items()))
            for word, postings in sorted(d.items())
        }
        
    def _merge_postings_limited(
        self, 
        current: BType,
        term: str, 
        p1: Dict[str, int], 
        p2: Dict[str, int]
    ) -> Tuple[bool, Dict[str, int], Optional[Dict[str, int]]]:
        """
        Intenta insertar postings fusionados de p1 y p2 para el término `term` dentro de `current`.
        Si no cabe todo, devuelve el fragmento que falta para ser insertado en otro bucket.
        
        Return:
            - inserted: bool → si algo se insertó
            - merged_partial: los postings que se insertaron
            - rest: el resto (None si todo fue insertado)
        """
        #print("==========")
        #print(current)
        #print(p1)
        #print(p2)
        all_docs = sorted(set(p1) | set(p2))
        merged = OrderedDict()
        rest = OrderedDict()

        for doc in all_docs:
            freq = p1.get(doc, 0) + p2.get(doc, 0)
            merged[doc] = freq
            temp = current.copy()
            temp[term] = dict(merged)
            if len(pickle.dumps(temp)) > BUCKET_LIMIT:
                # Saca el último doc y lo pasa a rest
                # El último doc agregado causó overflow, así que lo quitamos
                overflow_doc = list(merged.keys())[-1]
                merged.pop(overflow_doc)

                # Agregamos overflow_doc y todos los docs restantes a rest
                rest[overflow_doc] = p1.get(overflow_doc, 0) + p2.get(overflow_doc, 0)

                # Y agregamos todos los que no se procesaron aún
                remaining_docs = all_docs[all_docs.index(overflow_doc) + 1:]
                for doc in remaining_docs:
                    rest[doc] = p1.get(doc, 0) + p2.get(doc, 0)
                break

        inserted = bool(merged)
        rest = dict(rest) if rest else None
        #print(inserted) 
        #print(dict(merged))
        #print(rest)
        #print("-----------")
        return inserted, dict(merged), rest

    def _merge_until_limit(
        self, 
        current: BType, 
        b1: Optional[BType], 
        b2: Optional[BType],
        end_b1: bool,
        end_b2: bool
    ) -> Tuple[BType, Optional[BType], Optional[BType], bool, bool]:
        b1 = dict(b1) if b1 else {}
        b2 = dict(b2) if b2 else {}

        while b1 or b2:
            force_b1_done = not b1 and end_b1
            force_b2_done = not b2 and end_b2

            if b1 and (not b2 or next(iter(b1)) < next(iter(b2))) or force_b2_done:
                #print("AAAA")
                term = next(iter(b1))
                p1 = b1[term]
                p2 = {}

                inserted, merged_partial, rest_postings = self._merge_postings_limited(current, term, p1, p2)
                if not inserted:
                    return current, b1 or None, b2 or None, False, False

                current[term] = merged_partial

                if rest_postings:
                    b1[term] = rest_postings
                else:
                    del b1[term]

                if not b1 and not end_b1:
                    return current, None, b2 or None, True, False

            elif b2 and (not b1 or next(iter(b2)) < next(iter(b1))) or force_b1_done:
                #print("BBBB")
                term = next(iter(b2))
                p1 = {}
                p2 = b2[term]

                inserted, merged_partial, rest_postings = self._merge_postings_limited(current, term, p1, p2)
                if not inserted:
                    return current, b1 or None, b2 or None, False, False

                current[term] = merged_partial

                if rest_postings:
                    b2[term] = rest_postings
                else:
                    del b2[term]

                if not b2 and not end_b2:
                    return current, b1 or None, None, False, True

            else:
                #print("CCCC")
                term = next(iter(b1))
                p1 = b1[term]
                p2 = b2[term]

                inserted, merged_partial, rest_postings = self._merge_postings_limited(current, term, p1, p2)
                if not inserted:
                    return current, b1 or None, b2 or None, False, False

                current[term] = merged_partial

                if rest_postings:
                    del b1[term]
                    b2[term] = rest_postings
                else:
                    del b1[term]
                    del b2[term]

                if not b1 and not end_b1 or not b2 and not end_b2:
                    return current, b1 or None, b2 or None, not b1 and not end_b1, not b2 and not end_b2

        return current, None, None, False, False

    def _merge_bucket_range(self, l1: int, r1: int, l2: int, r2: int, output_file: InvertedFile):
        i, j = l1, l2
        b1 = self.file.read(i) if i <= r1 else None
        b2 = self.file.read(j) if j <= r2 else None
        current: BType = {}

        generated = 0

        while b1 or b2:
            end_b1 = i == r1
            end_b2 = j == r2

            current, b1, b2, advance_b1, advance_b2 = self._merge_until_limit(current, b1, b2, end_b1, end_b2)
            output_file.append(current)
            generated += 1
            current = {}

            if advance_b1 and i < r1:
                i += 1
                b1 = self.file.read(i)

            if advance_b2 and j < r2:
                j += 1
                b2 = self.file.read(j)


    def buildIndex(self):
        self.logger.info("Iniciando construcción del índice con SPIMI por rondas.")
        num_buckets = self.file._read_header()

        # Ordenar cada bucket individualmente
        for i in range(num_buckets):
            d = self.file.read(i)
            d_sorted = self._sort_dict(d)
            self.file.write(i, d_sorted)

        current_file = self.file
        current_name = self.filename
        round_num = 1

        # Inicializar rangos activos con cada bucket por separado
        active_ranges = [(i, i) for i in range(num_buckets)]

        while len(active_ranges) > 1:
            self.file.show()
            self.logger.info(f"--- Ronda #{round_num} ---")
            temp_name = current_name[:-4] + f"_tmp.dat"
            if os.path.exists(temp_name):
                os.remove(temp_name)

            output_file = InvertedFile(temp_name)
            new_ranges = []
            for idx in range(0, len(active_ranges), 2):
                if idx + 1 == len(active_ranges):
                    # No hay pareja, copiar directo
                    l, r = active_ranges[idx]
                    for i in range(l, r + 1):
                        output_file.append(current_file.read(i))
                    # Añadir este rango tal como está al nuevo conjunto
                    new_ranges.append((output_file._read_header() - (r - l + 1), output_file._read_header() - 1))
                else:
                    # Mergear los dos rangos
                    l1, r1 = active_ranges[idx]
                    l2, r2 = active_ranges[idx + 1]
                    start = output_file._read_header()
                    self._merge_bucket_range(l1, r1, l2, r2, output_file)
                    end = output_file._read_header() - 1
                    new_ranges.append((start, end))

            # Actualizar archivo y rangos
            os.remove(current_name)
            os.rename(temp_name, current_name)
            current_file = InvertedFile(current_name)
            self.file = current_file
            active_ranges = new_ranges
            round_num += 1

        self.logger.info("Índice invertido completamente construido y ordenado.")


    def getByWord(self,word)->Tuple[dict[int,int],int]:
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



import random

def generate_random_buckets(num_buckets=1024, terms_count=1000, docs_count=50, bucket_limit=BUCKET_LIMIT) -> List[Dict[str, Dict[str, int]]]:
    term_pool = [f"w{i}" for i in range(terms_count)]
    doc_pool = [f"d{i}" for i in range(docs_count)]

    buckets = []
    for _ in range(num_buckets):
        bucket = {}
        while True:
            term = random.choice(term_pool)
            postings = {}
            for _ in range(random.randint(1, 5)):
                doc = random.choice(doc_pool)
                postings[doc] = random.randint(1, 3)
            bucket[term] = postings
            # Probar si cabe
            if len(pickle.dumps(bucket)) > bucket_limit:
                del bucket[term]  # quitar el último que causó overflow
                break
        buckets.append(bucket)
    return buckets

def test_hard_merge():
    filename = "test_hard.dat"
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'a').close()

    buckets = generate_random_buckets()
    index = InvertedIndex(filename)
    index.insert_buckets(buckets)
    index.buildIndex()
    index.file.show()
    # Verificación del orden global de los términos
    all_terms = []
    num_buckets = index.file._read_header()

    for i in range(num_buckets):
        bucket = index.file.read(i)
        all_terms.extend(bucket.keys())

    if all_terms == sorted(all_terms):
        print("✅ Todos los términos están ordenados globalmente.")
    else:
        print("❌ Los términos no están ordenados globalmente.")


def test_merge_until_limit():
    b1 = {'w1': {'d0': 2, 'd1': 1, 'd3': 3, 'd5': 1}, 'w2': {'d1': 3, 'd6': 1, 'd7': 2}, 'w8': {'d1': 1, 'd2': 1, 'd3': 3}}
    b2 = {'w1': {'d0': 1, 'd1': 1, 'd8': 3}, 'w2': {'d0': 1, 'd6': 1, 'd8': 1}, 'w3': {'d0': 2, 'd5': 2, 'd7': 1, 'd8': 1}}
    
    print(b1)
    print(b2)
    print()
    
    index = InvertedIndex("test_hard.dat")
    cur, b1, b2, x1, x2 = index._merge_until_limit({}, b1, b2, False, False)
    
    print(cur)
    print(b1)
    print(b2)
    print()

    cur, b1, b2, y1, y2 = index._merge_until_limit({}, b1, b2, True, True)
    print(cur)
    print(b1)
    print(b2)

def manual_test():
    buckets = [
        {'w1': {'d0': 2, 'd1': 1, 'd3': 3, 'd5': 1}, 'w2': {'d1': 3, 'd6': 1, 'd7': 2}, 'w8': {'d1': 1, 'd2': 1, 'd3': 3}},
        {'w1': {'d0': 1, 'd1': 1, 'd8': 3}, 'w2': {'d0': 1, 'd6': 1, 'd8': 1}, 'w3': {'d0': 2, 'd5': 2, 'd7': 1, 'd8': 1}},
        {'w0': {'d4': 2}, 'w7': {'d0': 3, 'd3': 1, 'd6': 3, 'd7': 3}, 'w9': {'d0': 1, 'd2': 1, 'd4': 2, 'd8': 3}},
        {'w0': {'d2': 2, 'd3': 1, 'd4': 3}, 'w6': {'d1': 3, 'd6': 3, 'd8': 1}, 'w7': {'d2': 1, 'd3': 3, 'd4': 3, 'd7': 3, 'd8': 2}, 'w9': {'d8': 2}}
    ]
    filename = "test_manual.dat"
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'a').close()

    index = InvertedIndex(filename)
    index.insert_buckets(buckets)
    index.buildIndex()
    index.file.show()

def manual_test_2():
    buckets = [
        {'w0': {'d8': 1}, 'w2': {'d1': 2, 'd2': 3}, 'w9': {'d4': 2, 'd7': 3, 'd8': 3, 'd9': 1}},
        {'w1': {'d2': 3, 'd4': 3, 'd9': 1}, 'w6': {'d2': 1}, 'w7': {'d1': 3, 'd5': 1, 'd7': 2}},
        {'w3': {'d0': 3, 'd2': 1, 'd3': 3, 'd9': 3}, 'w5': {'d4': 2}},
        {'w0': {'d1': 2, 'd2': 3, 'd6': 1, 'd7': 2}, 'w6': {'d2': 2, 'd4': 3, 'd5': 3, 'd7': 3}, 'w9': {'d1': 3}},
    ]
    filename = "test_manual.dat"
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'a').close()

    index = InvertedIndex(filename)
    index.insert_buckets(buckets)
    index.buildIndex()
    index.file.show()
    
def manual_test_3():
    buckets = [
        {'w26': {'d1': 2, 'd28': 1, 'd8': 2}, 'w37': {'d17': 2, 'd23': 1, 'd27': 2}, 'w93': {'d12': 2, 'd19': 3, 'd4': 3}, 'w96': {'d11': 1, 'd13': 3, 'd24': 1, 'd25': 3, 'd5': 3}, 'w98': {'d29': 3, 'd6': 1, 'd8': 3}},
        {'w31': {'d1': 2, 'd20': 1}, 'w4': {'d13': 2, 'd18': 2}, 'w42': {'d16': 3, 'd21': 2}, 'w67': {'d15': 3, 'd29': 1}, 'w77': {'d24': 3, 'd27': 3, 'd9': 2}, 'w79': {'d14': 2, 'd15': 3, 'd3': 3}, 'w99': {'d12': 2}},
        {'w12': {'d17': 2, 'd2': 1}, 'w15': {'d16': 3, 'd25': 2}, 'w21': {'d1': 3}, 'w26': {'d18': 2}, 'w67': {'d12': 3, 'd14': 3, 'd4': 3}, 'w80': {'d1': 2, 'd16': 2, 'd18': 1}, 'w81': {'d5': 2, 'd7': 2, 'd8': 2}},
        {'w16': {'d0': 3, 'd25': 2, 'd26': 1, 'd5': 1}, 'w49': {'d13': 2, 'd15': 2, 'd23': 2, 'd29': 2}, 'w83': {'d11': 2, 'd18': 2}, 'w86': {'d13': 3, 'd5': 3}, 'w94': {'d15': 1, 'd4': 3, 'd7': 3}, 'w96': {'d7': 2}},
    ]
    filename = "test_manual.dat"
    if os.path.exists(filename):
        os.remove(filename)
    open(filename, 'a').close()

    index = InvertedIndex(filename)
    index.insert_buckets(buckets)
    index.buildIndex()
    index.file.show()

if __name__ == "__main__":
    test_hard_merge()


