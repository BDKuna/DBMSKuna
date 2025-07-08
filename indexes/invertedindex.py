import os
import sys
import pickle
import struct
from typing import Dict, List, Optional, Iterator, Tuple, OrderedDict

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from preprocessing.text_utils import bagOfWords
import math
import csv 
import logger

BUCKET_LIMIT = 1024

BType = Dict[str, Dict[str, int]]

DOC_HEADER_FORMAT = "i"       # Número de documentos
DOC_RECORD_FORMAT = "10si"    # (doc_id: str (10 bytes), term_count: int)
DOC_HEADER_SIZE = struct.calcsize(DOC_HEADER_FORMAT)
DOC_RECORD_SIZE = struct.calcsize(DOC_RECORD_FORMAT)

class DocumentFile:
    def __init__(self, filename: str):
        self.filename = filename
        self.logger = logger.CustomLogger(f"DOCUMENT-FILE-{filename}".upper())

        if not os.path.exists(filename):
            with open(filename, 'wb') as f:
                f.write(struct.pack(DOC_HEADER_FORMAT, 0))
            self.logger.info(f"Archivo {filename} creado con header.")

    def _read_header(self) -> int:
        with open(self.filename, 'rb') as f:
            data = f.read(DOC_HEADER_SIZE)
            return struct.unpack(DOC_HEADER_FORMAT, data)[0] if data else 0

    def _write_header(self, num_docs: int):
        with open(self.filename, 'r+b') as f:
            f.seek(0)
            f.write(struct.pack(DOC_HEADER_FORMAT, num_docs))

    def append(self, doc_id: str, term_count: int) -> int:
        num_docs = self._read_header()
        doc_id_encoded = doc_id.encode('utf-8')[:16].ljust(16, b'\x00')
        with open(self.filename, 'r+b') as f:
            f.seek(DOC_HEADER_SIZE + num_docs * DOC_RECORD_SIZE)
            f.write(struct.pack(DOC_RECORD_FORMAT, doc_id_encoded, term_count))
        self._write_header(num_docs + 1)
        self.logger.debug(f"Append: doc_id={doc_id}, term_count={term_count}")
        return num_docs

    def read(self, pos: int) -> Optional[tuple[str, int]]:
        with open(self.filename, 'rb') as f:
            f.seek(DOC_HEADER_SIZE + pos * DOC_RECORD_SIZE)
            data = f.read(DOC_RECORD_SIZE)
            if not data or len(data) < DOC_RECORD_SIZE:
                self.logger.warning(f"Intento de lectura inválida en posición {pos}")
                return None
            doc_id_raw, term_count = struct.unpack(DOC_RECORD_FORMAT, data)
            doc_id = doc_id_raw.rstrip(b'\x00').decode('utf-8')
            return doc_id, term_count

    def find(self, doc_id: str) -> Optional[int]:
        """Busca la cantidad de términos de un doc_id usando búsqueda binaria."""
        left = 0
        right = self._read_header() - 1

        while left <= right:
            mid = (left + right) // 2
            record = self.read(mid)
            if record is None:
                break

            current_id, term_count = record
            if current_id == doc_id:
                return term_count
            elif current_id < doc_id:
                left = mid + 1
            else:
                right = mid - 1

        return None

    def show(self):
        num_records = self._read_header()
        self.logger.info(f"Mostrando contenido de {self.filename} ({num_records} registros):")

        for i in range(num_records):
            record = self.read(i)
            print(f"Record {i}: {record}")


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
        self.docfile = DocumentFile(filename[:-8] + "_doc.dat")

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
        self.logger.warning("Iniciando construcción del índice con SPIMI por rondas.")
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
            self.logger.warning(f"--- Ronda #{round_num} ---")
            self.logger.warning(f"Number of buckets: {current_file._read_header()}")
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

    def _get_by_word(self, w: str) -> Tuple[dict[int,int],int]:
        """
        Busca en el índice invertido todos los documentos que contienen la palabra `w`.

        Asume que los buckets están ordenados por palabra.
        Hace búsqueda binaria por buckets, y luego lineal hacia izquierda y derecha
        mientras la palabra siga apareciendo.
        """
        low = 0
        high = self.file._read_header() - 1
        result = {}

        # Paso 1: búsqueda binaria para encontrar un bucket que contenga la palabra
        found_idx = -1
        while low <= high:
            mid = (low + high) // 2
            bucket = self.file.read(mid)
            if not bucket:
                break

            terms = list(bucket.keys())
            if not terms:
                break

            if w < terms[0]:
                high = mid - 1
            elif w > terms[-1]:
                low = mid + 1
            else:
                if w in bucket:
                    found_idx = mid
                    break
                # Búsqueda lineal dentro del rango del bucket
                for term in terms:
                    if term == w:
                        found_idx = mid
                        break
                if found_idx != -1:
                    break
                # Por convención, seguimos buscando a la izquierda
                high = mid - 1

        if found_idx == -1:
            return {}  # No se encontró la palabra

        # Paso 2: recorrer hacia la izquierda
        i = found_idx - 1
        while i >= 0:
            bucket = self.file.read(i)
            if w in bucket:
                for doc_id, tf in bucket[w].items():
                    result[doc_id] = result.get(doc_id, 0) + tf
                i -= 1
            else:
                break  # Ya no aparece la palabra

        # Paso 3: recorrer hacia la derecha (incluyendo el bucket encontrado)
        i = found_idx
        num_buckets = self.file._read_header()
        while i < num_buckets:
            bucket = self.file.read(i)
            if w in bucket:
                for doc_id, tf in bucket[w].items():
                    result[doc_id] = result.get(doc_id, 0) + tf
                i += 1
            else:
                break  # Ya no aparece la palabra

        return result, len(result.keys())

    def searchQuery(self, consulta: str, limit: int) -> list[str]:
        query_tf = bagOfWords(consulta)
        total_docs = self.docfile._read_header()
        vector_doc = [self._get_by_word(word) for word in query_tf]

        query_tf_idf = {}
        for (word, tf), (postings, df) in zip(query_tf.items(), vector_doc):
            idf = math.log((total_docs + 1) / (df + 1)) + 1  # Smooth IDF
            query_tf_idf[word] = tf * idf

        query_norm = math.sqrt(sum(value ** 2 for value in query_tf_idf.values()))

        score = {}

        # 2. Calcular TF-IDF para cada documento y su similitud
        for i, (word, tf_q) in enumerate(query_tf.items()):
            postings, df = vector_doc[i]
            idf = math.log((total_docs + 1) / (df + 1)) + 1  # Smooth IDF
            for doc_id, tf_d in postings.items():
                tfidf_d = tf_d * idf

                # Inicializa score y acumulador parcial
                if doc_id not in score:
                    score[doc_id] = {"dot": 0.0, "doc_norm_sq": 0.0}

                score[doc_id]["dot"] += tfidf_d * query_tf_idf[word]
                score[doc_id]["doc_norm_sq"] += tfidf_d ** 2

        # 3. Calcular similitud coseno
        result = []
        for doc_id, values in score.items():
            dot = values["dot"]
            doc_norm = math.sqrt(values["doc_norm_sq"])
            if query_norm > 0 and doc_norm > 0:
                sim = dot / (query_norm * doc_norm)
                result.append((sim, doc_id))

        # 4. Ordenar y devolver los doc_ids más similares
        result.sort(reverse=True)  # mayor similitud primero
        return [(doc_id, _) for _, doc_id in result[:limit]]


"""
INDEX_PATH   = '../preprocessing/table_column_texts.dat'  
CSV_PATH     = '../preprocessing/data2/mpst_full_data.csv'        

if __name__ == "__main__":
    index = InvertedIndex(INDEX_PATH)

    results = index.searchQuery("iron man tony stark captain america bucky burns", 5)
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

"""