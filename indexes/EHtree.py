# indices/EH.py

import os, sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import struct
import pickle
from core.schema import TableSchema, Column, IndexType
from core import utils
import logger
from core import stats
import hashlib

# -------------
# Clase Record
# -------------
class Record:
    """
    Representa un par (key, pointer) donde `pointer` es la posición
    física en el archivo de datos.
    """
    def __init__(self, key, pointer):
        self.key = key
        self.pointer = pointer

    def __repr__(self):
        return f"Record(key={self.key!r}, ptr={self.pointer!r})"

    def to_bytes(self):
        stats.count_write()
        return pickle.dumps((self.key, self.pointer))

    @classmethod
    def from_bytes(cls, data):
        stats.count_read()
        k, p = pickle.loads(data)
        return cls(k, p)


# --------------------------------------------
# Bucket en disco con chaining de overflow
# --------------------------------------------
class Bucket:
    HEADER_FMT  = "!ii8s"              # num_records, next_bucket_id, padding
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, bucket_id, capacity, file_manager):
        self.bucket_id      = bucket_id
        self.capacity       = capacity
        self.fm             = file_manager
        self.records        = []       # lista de Record
        self.next_bucket_id = -1

    def load(self):
        data = self.fm._read_raw(self.bucket_id)
        nrec, nxt, _ = struct.unpack(self.HEADER_FMT, data[:self.HEADER_SIZE])
        self.next_bucket_id = nxt
        self.records = []
        pos = self.HEADER_SIZE
        for _ in range(nrec):
            length = struct.unpack("!I", data[pos:pos+4])[0]
            pos += 4
            blob = data[pos:pos+length]
            pos += length
            self.records.append(Record.from_bytes(blob))

    def save(self):
        body = b""
        for r in self.records:
            rb = r.to_bytes()
            body += struct.pack("!I", len(rb)) + rb
        header = struct.pack(self.HEADER_FMT, len(self.records), self.next_bucket_id, b"\x00"*8)
        raw = header + body
        self.fm._write_raw(self.bucket_id, raw)

    def is_full(self):
        return len(self.records) >= self.capacity

    def insert(self, rec: Record) -> bool:
        self.load()
        if not self.is_full():
            self.records.append(rec)
            self.save()
            return True
        if self.next_bucket_id != -1:
            return self.fm.load_bucket(self.next_bucket_id).insert(rec)
        return False

    def search(self, key) -> Record | None:
        self.load()
        for r in self.records:
            if r.key == key:
                return r
        if self.next_bucket_id != -1:
            return self.fm.load_bucket(self.next_bucket_id).search(key)
        return None

    def delete(self, key) -> bool:
        self.load()
        for i, r in enumerate(self.records):
            if r.key == key:
                del self.records[i]
                self.save()
                return True
        if self.next_bucket_id != -1:
            deleted = self.fm.load_bucket(self.next_bucket_id).delete(key)
            if deleted:
                # si el overflow quedó vacío, enlazamos su siguiente y reciclamos
                ov = self.fm.load_bucket(self.next_bucket_id)
                if not ov.records:
                    self.next_bucket_id = ov.next_bucket_id
                    self.fm.delete_bucket(ov.bucket_id)
                    self.save()
                return True
        return False

    def get_all(self) -> list[Record]:
        self.load()
        out = list(self.records)
        if self.next_bucket_id != -1:
            out.extend(self.fm.load_bucket(self.next_bucket_id).get_all())
        return out


# ---------------------
# Nodo del árbol digital
# ---------------------
class TreeNode:
    def __init__(self, level: int, bit_prefix: str):
        self.level      = level
        self.bit_prefix = bit_prefix
        self.left       = None       # tipo TreeNode
        self.right      = None
        self.bucket_id  = None       # tipo int

    def is_leaf(self):
        return self.left is None and self.right is None


# ---------------------------------------------
# FileManager: maneja I/O de buckets y free-list
# ---------------------------------------------
class FileManager:
    HEADER_FMT  = "!ii8s"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(self, path: str, capacity: int):
        self.path     = path
        self.capacity = capacity
        if not os.path.exists(self.path):
            # inicializar header: next_bucket_id=0, capacidad
            with open(self.path, "wb") as f:
                f.write(struct.pack(self.HEADER_FMT, 0, self.capacity, b"\x00"*8))
                stats.count_write()
            self.next_bucket_id = 0
        else:
            with open(self.path, "rb") as f:
                nb, cap, _ = struct.unpack(self.HEADER_FMT, f.read(self.HEADER_SIZE))
                stats.count_read()
                self.next_bucket_id = nb
                self.capacity       = cap

    def _write_header(self):
        with open(self.path, "r+b") as f:
            f.seek(0)
            f.write(struct.pack(self.HEADER_FMT, self.next_bucket_id, self.capacity, b"\x00"*8))
            stats.count_write()

    def _bucket_size(self):
        # espacio fijo por bucket
        avg = 256
        return Bucket.HEADER_SIZE + self.capacity * (4 + avg)

    def _bucket_offset(self, bid: int):
        return self.HEADER_SIZE + bid * self._bucket_size()

    def _read_raw(self, bid: int) -> bytes:
        with open(self.path, "rb") as f:
            f.seek(self._bucket_offset(bid))
            stats.count_read()
            return f.read(self._bucket_size())

    def _write_raw(self, bid: int, data: bytes):
        if len(data) > self._bucket_size():
            raise ValueError("Bucket overflow")
        data = data.ljust(self._bucket_size(), b"\x00")
        with open(self.path, "r+b") as f:
            f.seek(self._bucket_offset(bid))
            f.write(data)
            stats.count_write()

    def create_bucket(self) -> Bucket:
        bid = self.next_bucket_id
        self.next_bucket_id += 1
        self._write_header()
        b = Bucket(bid, self.capacity, self)
        b.save()
        return b

    def load_bucket(self, bid: int) -> Bucket:
        return Bucket(bid, self.capacity, self)

    def delete_bucket(self, bid: int):
        # aquí podríamos implementar free-list si queremos reciclar buckets
        pass


# -----------------------
# La clase pública que
# exporta DBManager
# -----------------------
class ExtendibleHashTree:

    def __init__(self,
                 schema: TableSchema,
                 column: Column,
                 bucket_capacity: int = 4,
                 max_depth: int = 100):
        if column.index_type != IndexType.HASH:
            raise Exception("Column index type mismatch for HASH")
        self.logger = logger.CustomLogger(f"EHTREE-{schema.table_name}-{column.name}")
        self.schema = schema
        self.column = column
        self.bucket_capacity = bucket_capacity
        self.max_depth = max_depth
        self.M = 1 << max_depth

        # archivos de buckets (.db) e índice (.tree)
        self.data_path = utils.get_index_file_path(schema.table_name,
                                                   column.name,
                                                   IndexType.HASH)
        self.tree_path = self.data_path + ".tree"

        # prepara FileManager
        self.fm = FileManager(self.data_path, bucket_capacity)

        # carga o inicializa la raíz
        if os.path.exists(self.tree_path):
            with open(self.tree_path, "rb") as f:
                self.root = pickle.load(f)
        else:
            # Dos hojas iniciales 0 y 1
            self.root = TreeNode(0, "")
            left  = TreeNode(1, "0")
            right = TreeNode(1, "1")
            lb = self.fm.create_bucket()
            rb = self.fm.create_bucket()
            left.bucket_id  = lb.bucket_id
            right.bucket_id = rb.bucket_id
            self.root.left  = left
            self.root.right = right
            self._save_tree()

    def _save_tree(self):
        with open(self.tree_path, "wb") as f:
            pickle.dump(self.root, f)

    def _hash_bits(self, key) -> str:
        if isinstance(key, str):
            idx = int.from_bytes(hashlib.sha256(key.encode()).digest(), byteorder='little') % self.M
        else:
            idx = hash(key) % self.M
        return format(idx, f'0{self.max_depth}b')

    def _find_leaf_node(self, bits: str) -> TreeNode:
        node = self.root
        while not node.is_leaf():
            b = bits[node.level]
            node = node.left if b == '0' else node.right
        return node

    def insert(self, pointer: int, key) -> None:
        """
        pointer = posición física en el data file,
        key     = valor de la columna a indexar.
        """
        self.logger.warning(f"INSERTING: {key}")
        rec  = Record(key, pointer)
        bits = self._hash_bits(key)
        leaf = self._find_leaf_node(bits)
        b    = self.fm.load_bucket(leaf.bucket_id)
        if b.insert(rec):
            return
        # overflow o split
        if leaf.level >= self.max_depth:
            # encadenamos overflow
            ov = self.fm.create_bucket()
            b.load()
            b.next_bucket_id = ov.bucket_id
            b.save()
            ov.insert(rec)
            return
        self._split_leaf(leaf, new_rec=rec)

    def _split_leaf(self, leaf: TreeNode, new_rec: Record=None, recs_list=None):
        # recolección
        if recs_list is None:
            temp = self.fm.load_bucket(leaf.bucket_id)
            recs = temp.get_all()
            if new_rec: recs.append(new_rec)
        else:
            recs = recs_list

        depth = leaf.level + 1
        lc = TreeNode(depth, leaf.bit_prefix + '0')
        rc = TreeNode(depth, leaf.bit_prefix + '1')
        leaf.left, leaf.right = lc, rc
        leaf.bucket_id = None

        r0, r1 = [], []
        for r in recs:
            bit = self._hash_bits(r.key)[leaf.level]
            (r0 if bit=='0' else r1).append(r)

        b0 = self.fm.create_bucket()
        b1 = self.fm.create_bucket()
        lc.bucket_id = b0.bucket_id
        rc.bucket_id = b1.bucket_id

        # rellenar o resplit recursivamente
        if len(r0) <= self.bucket_capacity:
            for x in r0: b0.insert(x)
        else:
            self._split_leaf(lc, recs_list=r0)

        if len(r1) <= self.bucket_capacity:
            for x in r1: b1.insert(x)
        else:
            self._split_leaf(rc, recs_list=r1)

        self._save_tree()

    def search(self, key) -> list[int]:
        """
        Igualdad exacta; devuelve lista (vacía o con un solo ptr).
        """
        self.logger.warning(f"SEARCHING: {key}")
        bits = self._hash_bits(key)
        leaf = self._find_leaf_node(bits)
        cosa = self.fm.load_bucket(leaf.bucket_id)
        r    = cosa.search(key)
        return [] if r is None else [r.pointer]

    def rangeSearch(self, lo, hi) -> list[int]:
        """
        Busca todos los registros con lo <= key <= hi.
        """

        if(lo == None):
            lo = utils.get_min_value(self.column)
        if(hi == None):
            hi = utils.get_max_value(self.column)
        self.logger.warning(f"RANGE-SEARCH: {lo}, {hi}")
        out = []
        # recorro todos y filtro en memoria:
        for rec in self.get_all():
            if lo <= rec.key <= hi:
                out.append(rec.pointer)
        return out

    def delete(self, key) -> None:
        """
        Elimina (key) si existe. No reequilibra profundidad de árbol.
        """
        self.logger.warning(f"DELETING: {key}")
        bits = self._hash_bits(key)
        leaf = self._find_leaf_node(bits)
        b    = self.fm.load_bucket(leaf.bucket_id)
        if b.delete(key):
            # opcional: podríamos colapsar hojas vacías (_merge_leaf)
            self._save_tree()
            return
        # si no existía, simplemente no hace nada
    
    def get_all(self) -> list[Record]:
        """
        Recorre todo el árbol y devuelve la lista de Record(key, pointer)
        sin convertirlos aún en punteros.
        """
        recs: list[Record] = []
        def dfs(node: TreeNode):
            if node.is_leaf():
                recs.extend(self.fm.load_bucket(node.bucket_id).get_all())
            else:
                dfs(node.left)
                dfs(node.right)

        dfs(self.root)
        return recs

    def getAll(self) -> list[int]:
        """
        Devuelve todos los punteros en orden de clave ascendente.
        """
        self.logger.warning(f"GET ALL RECORDS")
        recs = []
        def dfs(n: TreeNode):
            if n.is_leaf():
                recs.extend(self.fm.load_bucket(n.bucket_id).get_all())
            else:
                dfs(n.left); dfs(n.right)
        dfs(self.root)
        # ordenar por key
        recs.sort(key=lambda r: r.key)
        return [r.pointer for r in recs]

    def close(self):
        # opcionalmente asegurar persistencia
        self._save_tree()

    def clear(self):
        self.logger.info("Cleaning data, removing files")
        os.remove(self.data_path)
        os.remove(self.tree_path)
