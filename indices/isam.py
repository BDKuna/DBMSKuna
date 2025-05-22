import struct
import os
import sys
import bisect
import math

# Asegurar acceso a core
root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root not in sys.path:
    sys.path.insert(0, root)

from core import utils
from core.schema import IndexType
from core.record_file import RecordFile

# ------------------------
# Constantes de formato
# ------------------------
GLOBAL_HDR = "ii"     
GLOBAL_HDR_SIZE = struct.calcsize(GLOBAL_HDR)

# ------------------------
# Registro hoja y página
# ------------------------
class LeafRecord:
    FMT = "ii"  # key, data_pos
    SIZE = struct.calcsize(FMT)

    def __init__(self, key: int, pos: int):
        self.key = key
        self.pos = pos

    def pack(self) -> bytes:
        return struct.pack(self.FMT, self.key, self.pos)

    @staticmethod
    def unpack(data: bytes):
        k, p = struct.unpack(LeafRecord.FMT, data)
        return LeafRecord(k, p)

class LeafPage:
    HDR = "ii"  # page_num, next_page
    HSIZE = struct.calcsize(HDR)

    def __init__(self, page_num: int, next_page: int, records: list[LeafRecord], factor: int):
        self.page_num = page_num
        self.next_page = next_page
        self.factor = factor
        self.records = sorted(records, key=lambda r: r.key)

    def pack(self) -> bytes:
        data = struct.pack(self.HDR, self.page_num, self.next_page)
        for r in self.records:
            data += r.pack()
        # padding
        empty = self.factor - len(self.records)
        data += b''.join(b"\x00" * LeafRecord.SIZE for _ in range(empty))
        return data

    @staticmethod
    def unpack(data: bytes, factor: int):
        need = LeafPage.HSIZE + factor * LeafRecord.SIZE
        if len(data) < need:
            return None
        pn, nx = struct.unpack(LeafPage.HDR, data[:LeafPage.HSIZE])
        recs = []
        body = data[LeafPage.HSIZE:need]
        for i in range(factor):
            chunk = body[i*LeafRecord.SIZE:(i+1)*LeafRecord.SIZE]
            k, p = struct.unpack(LeafRecord.FMT, chunk)
            if k != 0 or p != 0:
                recs.append(LeafRecord(k, p))
        return LeafPage(pn, nx, recs, factor)

# ------------------------
# ISAM-Sparse: root + hojas
# ------------------------
class ISAMSparse:
    def __init__(self, data_file: str, index_file: str, leaf_factor: int = 4):
        self.data_file = data_file
        self.index_file = index_file
        self.factor = leaf_factor
        # inicializar índice si no existe
        if not os.path.exists(index_file) or os.path.getsize(index_file) < GLOBAL_HDR_SIZE:
            with open(index_file, 'wb') as f:
                f.write(b"\x00" * GLOBAL_HDR_SIZE)
            self.leaf_pages = []
            self.root = []
        else:
            self._load()

    def _load(self):
        leaf_size = LeafPage.HSIZE + self.factor * LeafRecord.SIZE
        size = os.path.getsize(self.index_file)
        self.leaf_pages = []
        with open(self.index_file, 'rb') as f:
            f.seek(GLOBAL_HDR_SIZE)
            page = 0
            while f.tell() + leaf_size <= size:
                data = f.read(leaf_size)
                lp = LeafPage.unpack(data, self.factor)
                if lp is None:
                    break
                self.leaf_pages.append(lp)
                page += 1
        # construir root como lista sparse
        self.root = [(lp.records[0].key, lp.page_num)
                     for lp in self.leaf_pages if lp.records]

    def _write_leaf(self, lp: LeafPage):
        leaf_size = LeafPage.HSIZE + self.factor * LeafRecord.SIZE
        offset = GLOBAL_HDR_SIZE + lp.page_num * leaf_size
        with open(self.index_file, 'r+b') as f:
            f.seek(offset)
            f.write(lp.pack())

    def build_index(self):
        """Reconstrucción offline desde data_file."""
        rf = RecordFile(self.data_file)
        all_records = list(rf.read_all())
        leaf_recs = [LeafRecord(r.id, i)
                     for i, r in enumerate(all_records)]
        pages = math.ceil(len(leaf_recs) / self.factor)
        # reiniciar archivo
        with open(self.index_file, 'wb') as f:
            f.write(b"\x00" * GLOBAL_HDR_SIZE)
        with open(self.index_file, 'ab') as f:
            for p in range(pages):
                chunk = leaf_recs[p*self.factor:(p+1)*self.factor]
                nxt = p+1 if p+1 < pages else -1
                lp = LeafPage(p, nxt, chunk, self.factor)
                f.write(lp.pack())
        self._load()

    def insert(self, key: int, pos: int):
        lr = LeafRecord(key, pos)
        if not self.leaf_pages:
            lp = LeafPage(0, -1, [lr], self.factor)
            with open(self.index_file, 'r+b') as f:
                f.seek(GLOBAL_HDR_SIZE)
                f.write(lp.pack())
            self.leaf_pages = [lp]
            self.root = [(key, 0)]
            return
        # localizar hoja via root
        keys = [k for k, _ in self.root]
        idx = bisect.bisect_right(keys, key) - 1
        if idx < 0:
            idx = 0
        pn = self.root[idx][1]
        lp = self.leaf_pages[pn]
        if len(lp.records) < self.factor:
            lp.records.append(lr)
            lp.records.sort(key=lambda r: r.key)
            self._write_leaf(lp)
        else:
            # split
            allr = sorted(lp.records + [lr], key=lambda r: r.key)
            mid = len(allr) // 2
            left, right = allr[:mid], allr[mid:]
            lp.records = left
            newp = len(self.leaf_pages)
            lp.next_page = newp
            self._write_leaf(lp)
            lp2 = LeafPage(newp, -1, right, self.factor)
            with open(self.index_file, 'r+b') as f:
                f.seek(0, os.SEEK_END)
                f.write(lp2.pack())
            self.leaf_pages.append(lp2)
        # rebuild root
        self.root = [(l.records[0].key, l.page_num)
                     for l in self.leaf_pages if l.records]

    def search(self, key: int) -> int | None:
        if not self.root:
            return None
        keys = [k for k, _ in self.root]
        idx = bisect.bisect_right(keys, key) - 1
        if idx < 0:
            idx = 0
        lp = self.leaf_pages[self.root[idx][1]]
        for r in lp.records:
            if r.key == key:
                return r.pos
        return None

    def range_search(self, lo: int, hi: int) -> list[int]:
        out = []
        if not self.root:
            return out
        keys = [k for k, _ in self.root]
        idx = bisect.bisect_right(keys, lo) - 1
        if idx < 0:
            idx = 0
        for lp in self.leaf_pages[idx:]:
            for r in lp.records:
                if r.key < lo:
                    continue
                if r.key > hi:
                    return out
                out.append(r.pos)
        return out

    def delete(self, key: int) -> bool:
        pos = self.search(key)
        if pos is None:
            return False
        # eliminar de la hoja
        for lp in self.leaf_pages:
            for i, r in enumerate(lp.records):
                if r.key == key:
                    lp.records.pop(i)
                    self._write_leaf(lp)
                    self.root = [(l.records[0].key, l.page_num)
                                 for l in self.leaf_pages if l.records]
                    return True
        return False

# ------------------------
# Adaptador para DBManager
# ------------------------
class ISAMIndex:
    def __init__(self, table_schema, column):
        # Ruta de datos y index
        self.rf = RecordFile(table_schema)
        data_file = self.rf.filename
        index_file = utils.get_index_file_path(
            table_schema.table_name,
            column.name,
            IndexType.ISAM
        )
        self.core = ISAMSparse(data_file, index_file)

    def insert(self, pos: int, key):
        self.core.insert(int(key), pos)

    def search(self, key):
        res = self.core.search(int(key))
        return [] if res is None else [res]

    def rangeSearch(self, a, b):
        return self.core.range_search(int(a), int(b))

    def getAll(self):
        return [r.pos for r in self.core.leaf_pages for r in r.records]

    def delete(self, key):
        return self.core.delete(int(key))
