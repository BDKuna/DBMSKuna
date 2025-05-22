import os
import struct
import bisect
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.schema import TableSchema, Column, IndexType
from core.utils import calculate_column_format, get_index_file_path
from core.record_file import RecordFile

class BlockSummary:
    """
    Resumen de un bloque de registros físicos:
    - start_pos, end_pos: rango de posiciones
    - min_value, max_value: valores mínimo y máximo indexados
    - count: número de registros válidos
    """
    def __init__(self, start_pos, end_pos, min_value, max_value, count):
        self.start_pos = start_pos
        self.end_pos = end_pos
        self.min_value = min_value
        self.max_value = max_value
        self.count = count

    def contains(self, value):
        return self.min_value <= value <= self.max_value

class BRINFile:
    """
    Persiste los resúmenes de bloque en un archivo binario de formato fijo.
    Estructura:
      HEADER: num_blocks (I)
      ENTRIES: por cada bloque -> start_pos (I), end_pos (I), min_value, max_value, count (I)
    """
    def __init__(self, schema: TableSchema, column: Column):
        self.schema = schema
        self.column = column
        self.filename = get_index_file_path(schema.table_name, column.name, IndexType.BRIN)
        fmt = calculate_column_format(column)
        self.header_struct = struct.Struct('<I')
        self.entry_struct = struct.Struct(f'<I I {fmt} {fmt} I')
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)
        if not os.path.exists(self.filename):
            with open(self.filename, 'wb') as f:
                f.write(self.header_struct.pack(0))

    def read_blocks(self):
        blocks = []
        with open(self.filename, 'rb') as f:
            hdr = f.read(self.header_struct.size)
            if not hdr:
                return blocks
            num = self.header_struct.unpack(hdr)[0]
            for _ in range(num):
                data = f.read(self.entry_struct.size)
                sp, ep, mn, mx, cnt = self.entry_struct.unpack(data)
                # si es varchar, descartar padding
                if isinstance(mn, (bytes, bytearray)):
                    mn = mn.rstrip(b'\x00').decode()
                    mx = mx.rstrip(b'\x00').decode()
                blocks.append(BlockSummary(sp, ep, mn, mx, cnt))
        return blocks

    def write_blocks(self, blocks):
        with open(self.filename, 'wb') as f:
            f.write(self.header_struct.pack(len(blocks)))
            for blk in blocks:
                mn, mx = blk.min_value, blk.max_value
                # varchar a bytes con padding
                if isinstance(mn, str):
                    length = struct.calcsize(calculate_column_format(self.column))
                    mn = mn.encode().ljust(length, b'\x00')
                    mx = mx.encode().ljust(length, b'\x00')
                f.write(self.entry_struct.pack(
                    blk.start_pos, blk.end_pos, mn, mx, blk.count
                ))

class BRINIndex:
    """
    Índice BRIN genérico con:
    - Formato binario de bloques
    - Splits y merges automáticos
    - Búsqueda por binary-search sobre resúmenes
    """
    def __init__(self, schema: TableSchema, column: Column, block_size: int = 128):
        if column.index_type != IndexType.BRIN:
            raise ValueError('Column must be BRIN index type')
        self.schema = schema
        self.column = column
        self.block_size = block_size
        self.file = BRINFile(schema, column)
        self.blocks = self.file.read_blocks()
        # Ordenar blocks por min_value
        self.blocks.sort(key=lambda b: b.min_value)

    def _save(self):
        self.blocks.sort(key=lambda b: b.min_value)
        self.file.write_blocks(self.blocks)

    def _col_idx(self):
        for i, col in enumerate(self.schema.columns):
            if col.name == self.column.name:
                return i
        raise ValueError('Column not found in schema')

    def _find_block(self, value):
        """Binary-search para encontrar índice de bloque que podría contener 'value'."""
        keys = [b.min_value for b in self.blocks]
        i = bisect.bisect_right(keys, value) - 1
        if i >= 0 and self.blocks[i].contains(value):
            return i
        return None

    def _split_block(self, idx):
        blk = self.blocks[idx]
        rf = RecordFile(self.schema)
        ci = self._col_idx()
        # recolectar pares (pos, val)
        pairs = [(p, rf.read(p).values[ci]) for p in range(blk.start_pos, blk.end_pos + 1)
                 if rf.read(p) and rf.read(p).values[ci] is not None]
        if len(pairs) <= self.block_size:
            return
        # dividir en mitades
        half = len(pairs) // 2
        left, right = pairs[:half], pairs[half:]
        def make_block(chunk):
            ps, vs = zip(*chunk)
            return BlockSummary(min(ps), max(ps), min(vs), max(vs), len(chunk))
        self.blocks[idx] = make_block(left)
        self.blocks.insert(idx+1, make_block(right))

    def _merge_blocks(self):
        i = 0
        while i < len(self.blocks)-1:
            a, b = self.blocks[i], self.blocks[i+1]
            # merge si total < block_size
            if a.count + b.count < self.block_size:
                merged = BlockSummary(
                    start_pos = a.start_pos,
                    end_pos   = b.end_pos,
                    min_value = min(a.min_value, b.min_value),
                    max_value = max(a.max_value, b.max_value),
                    count     = a.count + b.count
                )
                self.blocks[i] = merged
                del self.blocks[i+1]
            else:
                i += 1

    def insert(self, position: int, value):
        """Insertar valor en posición y ajustar bloques."""
        bi = self._find_block(value)
        if bi is not None:
            blk = self.blocks[bi]
            blk.min_value = min(blk.min_value, value)
            blk.max_value = max(blk.max_value, value)
            blk.end_pos = max(blk.end_pos, position)
            blk.count += 1
            self._split_block(bi)
        else:
            # nuevo bloque
            new_blk = BlockSummary(position, position, value, value, 1)
            bisect.insort(self.blocks, new_blk, key=lambda b: b.min_value)
        self._save()

    def search(self, key):
        """Devuelve posiciones con valor == key"""
        rf = RecordFile(self.schema)
        ci = self._col_idx()
        bi = self._find_block(key)
        if bi is None:
            return []
        blk = self.blocks[bi]
        res = []
        for p in range(blk.start_pos, blk.end_pos+1):
            rec = rf.read(p)
            if rec and rec.values[ci] == key:
                res.append(p)
        return res

    def rangeSearch(self, low, high):
        """Devuelve posiciones en rango [low, high]"""
        rf = RecordFile(self.schema)
        ci = self._col_idx()
        # encontrar primer bloque con max_value >= low
        res = []
        for blk in self.blocks:
            if blk.max_value < low:
                continue
            if blk.min_value > high:
                break
            for p in range(blk.start_pos, blk.end_pos+1):
                rec = rf.read(p)
                if rec and low <= rec.values[ci] <= high:
                    res.append(p)
        return res

    def delete(self, key):
        """Eliminar registros con valor == key y ajustar bloques."""
        rf = RecordFile(self.schema)
        ci = self._col_idx()
        bi = self._find_block(key)
        if bi is None:
            return False
        blk = self.blocks[bi]
        vals = []
        deleted = False
        for p in range(blk.start_pos, blk.end_pos+1):
            rec = rf.read(p)
            if rec and rec.values[ci] == key:
                rf.delete(p)
                deleted = True
            elif rec and rec.values[ci] is not None:
                vals.append((p, rec.values[ci]))
        if not deleted:
            return False
        if vals:
            ps, vs = zip(*vals)
            self.blocks[bi] = BlockSummary(min(ps), max(ps), min(vs), max(vs), len(ps))
        else:
            del self.blocks[bi]
        # intentar merge
        self._merge_blocks()
        self._save()
        return True

    def update(self, key, new_record):
        """Actualizar registro con valor == key."""
        rf = RecordFile(self.schema)
        ci = self._col_idx()
        pos = 0
        found = False
        while True:
            try:
                rec = rf.read(pos)
            except Exception:
                break
            if rec and rec.values[ci] == key:
                rf.delete(pos)
                new_pos = rf.append(new_record)
                new_key = new_record.values[ci]
                self.insert(new_pos, new_key)
                found = True
                break
            pos += 1
        return found

    def getAll(self):
        """Devuelve todas las posiciones válidas"""
        rf = RecordFile(self.schema)
        ci = self._col_idx()
        res = []
        pos = 0
        while True:
            try:
                rec = rf.read(pos)
            except Exception:
                break
            if rec and rec.values[ci] is not None:
                res.append(pos)
            pos += 1
        return res
