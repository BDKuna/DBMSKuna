# indices/isam.py

import os, struct, bisect
from core.schema import TableSchema, Column, IndexType, DataType
from core import utils
from core.record_file import RecordFile, Record
import logger

# --------------------------------------------------------------------
# 1) Record + Page definitions
# --------------------------------------------------------------------

class LeafRecord:
    def __init__(self, column: Column, key, datapos: int):
        self.column = column
        self.key     = key
        self.datapos = datapos
        self.FMT     = utils.calculate_column_format(column) + "i"
        self.STRUCT  = struct.Struct(self.FMT)
        self.SIZE    = self.STRUCT.size

    def pack(self):
        val = self.key.encode() if self.column.data_type == DataType.VARCHAR else self.key
        return self.STRUCT.pack(val, self.datapos)

    @staticmethod
    def unpack(buf: bytes, column: Column):
        fmt      = utils.calculate_column_format(column) + "i"
        key, dp  = struct.unpack(fmt, buf)
        if column.data_type == DataType.VARCHAR:
            key = key.decode().rstrip("\x00")
        return LeafRecord(column, key, dp)


class IndexRecord:
    def __init__(self, column: Column, key, left: int, right: int):
        self.column = column
        self.key    = key
        self.left   = left
        self.right  = right
        self.FMT    = utils.calculate_column_format(column) + "ii"
        self.STRUCT = struct.Struct(self.FMT)
        self.SIZE   = self.STRUCT.size

    def pack(self):
        val = self.key.encode() if self.column.data_type == DataType.VARCHAR else self.key
        return self.STRUCT.pack(val, self.left, self.right)

    @staticmethod
    def unpack(buf: bytes, column: Column):
        fmt       = utils.calculate_column_format(column) + "ii"
        key, l, r = struct.unpack(fmt, buf)
        if column.data_type == DataType.VARCHAR:
            key = key.decode().rstrip("\x00")
        return IndexRecord(column, key, l, r)


class LeafPage:
    HEADER_FMT = "iii"  # page_num, next_page, not_overflow
    HSIZE      = struct.calcsize(HEADER_FMT)

    def __init__(self, page_num, next_page, not_overflow, records, leaf_factor):
        self.page_num     = page_num
        self.next_page    = next_page
        self.not_overflow = not_overflow
        self.records      = records
        self.leaf_factor  = leaf_factor
        # struct = cabecera + leaf_factor * registro
        self.STRUCT       = struct.Struct(self.HEADER_FMT + "".join(r.FMT for r in records))

    def pack(self):
        hdr = (self.page_num, self.next_page, self.not_overflow)
        data = list(hdr)
        for rec in self.records:
            data.extend(rec.STRUCT.unpack(rec.pack()))
        return self.STRUCT.pack(*data)


class IndexPage:
    HEADER_FMT = "i"   # page_num
    HSIZE      = struct.calcsize(HEADER_FMT)

    def __init__(self, page_num, records, index_factor):
        self.page_num     = page_num
        self.records      = records
        self.index_factor = index_factor
        fmt = self.HEADER_FMT + "".join(r.FMT for r in records)
        self.STRUCT = struct.Struct(fmt)

    def pack(self):
        data = [self.page_num]
        for rec in self.records:
            data.extend(rec.STRUCT.unpack(rec.pack()))
        return self.STRUCT.pack(*data)


# --------------------------------------------------------------------
# 2) ISAMFile: acceso al disco
# --------------------------------------------------------------------

class ISAMFile:
    HEADER_FMT = "ii"  # leaf_factor, index_factor
    HEADER_SIZE = struct.calcsize(HEADER_FMT)
    HEADER_STRUCT = struct.Struct(HEADER_FMT)

    def __init__(self, schema: TableSchema, column: Column,
                 leaf_factor: int = 4, index_factor: int = 4):
        if column.index_type != IndexType.ISAM:
            raise Exception("column index type doesn't match ISAM")
        self.schema       = schema
        self.column       = column
        self.leaf_factor  = leaf_factor
        self.index_factor = index_factor
        self.filename     = utils.get_index_file_path(schema.table_name,
                                                     column.name,
                                                     IndexType.ISAM)
        # formatos por registro
        key_fmt           = utils.calculate_column_format(column)
        self.leaf_struct  = struct.Struct(key_fmt + "i")
        self.index_struct = struct.Struct(key_fmt + "ii")

        # crear fichero si no existe
        if not os.path.exists(self.filename):
            open(self.filename, "wb").close()
        # escribir header
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(self.HEADER_STRUCT.pack(leaf_factor, index_factor))

    def read_header(self):
        with open(self.filename, "rb") as f:
            lf, ix = self.HEADER_STRUCT.unpack(f.read(self.HEADER_SIZE))
            return lf, ix

    def read_root_page(self) -> IndexPage:
        # ROOT empieza justo después del header global
        _, ix = self.read_header()
        root_size = IndexPage.HSIZE + ix * IndexRecord.SIZE
        with open(self.filename, "rb") as f:
            f.seek(self.HEADER_SIZE)
            buf = f.read(root_size)
        # desempaquetar registros
        recs = []
        off = IndexPage.HSIZE
        for _ in range(ix):
            recs.append(IndexRecord.unpack(buf[off:off+IndexRecord.SIZE], self.column))
            off += IndexRecord.SIZE
        page_num = struct.unpack(IndexPage.HEADER_FMT, buf[:IndexPage.HSIZE])[0]
        return IndexPage(page_num, recs, ix)

    def write_root_page(self, root: IndexPage):
        with open(self.filename, "r+b") as f:
            # sitúa justo tras el header global
            f.seek(self.HEADER_SIZE)
            f.write(root.pack())

    def read_level1_page(self, lvl1_num: int) -> IndexPage:
        lf, ix = self.read_header()
        root_size   = IndexPage.HSIZE + ix * IndexRecord.SIZE
        lvl1_size   = root_size  # mismo formato que root
        offset      = self.HEADER_SIZE + root_size + lvl1_num * lvl1_size
        with open(self.filename, "rb") as f:
            f.seek(offset)
            buf = f.read(lvl1_size)
        # desempacar igual que root
        recs = []
        off = IndexPage.HSIZE
        for _ in range(ix):
            recs.append(IndexRecord.unpack(buf[off:off+IndexRecord.SIZE], self.column))
            off += IndexRecord.SIZE
        page_num = struct.unpack(IndexPage.HEADER_FMT, buf[:IndexPage.HSIZE])[0]
        return IndexPage(page_num, recs, ix)

    def write_level1_page(self, page: IndexPage):
        lf, ix = self.read_header()
        root_size = IndexPage.HSIZE + ix * IndexRecord.SIZE
        offset    = self.HEADER_SIZE + root_size + page.page_num * (root_size)
        with open(self.filename, "r+b") as f:
            f.seek(offset)
            f.write(page.pack())

    def read_leaf_page(self, leaf_num: int) -> LeafPage:
        lf, ix    = self.read_header()
        root_size = IndexPage.HSIZE + ix * IndexRecord.SIZE
        level1_size = root_size * (ix + 1)
        leaf_size = LeafPage.HSIZE + lf * LeafRecord.SIZE
        offset    = self.HEADER_SIZE + level1_size + leaf_num * leaf_size
        with open(self.filename, "rb") as f:
            f.seek(offset)
            buf = f.read(leaf_size)
        # desempacar
        hdr       = buf[:LeafPage.HSIZE]
        page_num, nxt, not_ovf = struct.unpack(LeafPage.HEADER_FMT, hdr)
        recs      = []
        off       = LeafPage.HSIZE
        for _ in range(lf):
            recs.append(LeafRecord.unpack(buf[off:off+LeafRecord.SIZE], self.column))
            off += LeafRecord.SIZE
        return LeafPage(page_num, nxt, not_ovf, recs, lf)

    def write_leaf_page(self, page: LeafPage):
        lf, ix    = self.read_header()
        root_size = IndexPage.HSIZE + ix * IndexRecord.SIZE
        level1_size = root_size * (ix + 1)
        leaf_size = LeafPage.HSIZE + lf * LeafRecord.SIZE
        offset    = self.HEADER_SIZE + level1_size + page.page_num * leaf_size
        with open(self.filename, "r+b") as f:
            f.seek(offset)
            f.write(page.pack())


# --------------------------------------------------------------------
# 3) ISAMIndex: lógica del índice
# --------------------------------------------------------------------

class ISAMIndex:
    def __init__(self, schema: TableSchema, column: Column,
                 leaf_factor: int = 4, index_factor: int = 4):
        self.schema = schema
        self.column = column
        self.rf     = RecordFile(schema)
        self.file   = ISAMFile(schema, column, leaf_factor, index_factor)
        self.logger = logger.CustomLogger(f"ISAMINDEX-{schema.table_name}-{column.name}")

    def build_index(self):
        # aquí llamas a tus copy_to_leaf_records(), _build_level1(), _build_root(), etc.
        ...

    def search(self, key) -> list[int]:
        ...

    def delete(self, key) -> None:
        ...

    def rangeSearch(self, lo, hi) -> list[int]:
        ...

    def getAll(self) -> list[int]:
        ...

    def clear(self) -> None:
        ...