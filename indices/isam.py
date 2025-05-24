# indices/isam.py

import os, struct, bisect, math
from core.schema import TableSchema, Column, IndexType, DataType
from core import utils
from core.record_file import RecordFile, Record
import logger

# --------------------------------------------------------------------
# 1) Record + Page definitions
# --------------------------------------------------------------------

class LeafRecord:
    # clave (tipo según columna) + posición de datos (int)
    def __init__(self, column: Column, key, datapos: int):
        self.FMT = utils.calculate_column_format(column) + "i"
        self.STRUCT = struct.Struct(self.FMT)
        self.key = key
        self.datapos = datapos

    def pack(self) -> bytes:
        # varchar → encode+padded, demás → pasarlo directamente
        val = self.key.encode() if self.STRUCT.format[0].isalpha() else self.key
        return self.STRUCT.pack(val, self.datapos)

    @staticmethod
    def unpack(column: Column, data: bytes) -> 'LeafRecord':
        FMT = utils.calculate_column_format(column) + "i"
        val, datapos = struct.unpack(FMT, data)
        if column.data_type == utils.DataType.VARCHAR:
            val = val.decode().rstrip("\x00")
        return LeafRecord(column, val, datapos)

class IndexRecord:
    # clave + dos punteros (int,int)
    def __init__(self, column: Column, key, left: int, right: int):
        self.FMT = utils.calculate_column_format(column) + "ii"
        self.STRUCT = struct.Struct(self.FMT)
        self.key = key
        self.left = left
        self.right = right

    def pack(self) -> bytes:
        val = self.key.encode() if self.STRUCT.format[0].isalpha() else self.key
        return self.STRUCT.pack(val, self.left, self.right)

    @staticmethod
    def unpack(column: Column, data: bytes) -> 'IndexRecord':
        FMT = utils.calculate_column_format(column) + "ii"
        key, left, right = struct.unpack(FMT, data)
        if column.data_type == utils.DataType.VARCHAR:
            key = key.decode().rstrip("\x00")
        return IndexRecord(column, key, left, right)

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

    @classmethod
    def pack_header(cls, page_num, next_page, not_overflow):
        return struct.pack(cls.HEADER_FMT, page_num, next_page, not_overflow)


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

    def find_child_ptr(self, key):
        """
        Given a search key, pick the child pointer to follow.
        We assume self.records is ordered by record.key ascending,
        and each record partitions the key space:
          if key < record.key ⇒ go to record.left,
        otherwise at end ⇒ use last record.right.
        """
        for rec in self.records:
            if key < rec.key:
                return rec.left
        # fell through ⇒ all rec.key ≤ key ⇒ go right of the last record
        return self.records[-1].right


# --------------------------------------------------------------------
# 2) ISAMFile: acceso al disco
# --------------------------------------------------------------------

class ISAMFile:
    HEADER_FMT    = "ii"  # leaf_factor, index_factor
    HEADER_STRUCT = struct.Struct(HEADER_FMT)
    HEADER_SIZE   = HEADER_STRUCT.size

    def __init__(self,
                 schema: TableSchema,
                 column: Column,
                 leaf_factor: int  = 4,
                 index_factor: int = 4):
        if column.index_type != IndexType.ISAM:
            raise Exception("column index type no coincide con ISAM")
        self.schema       = schema
        self.column       = column
        self.leaf_factor  = leaf_factor
        self.index_factor = index_factor
        self.filename     = utils.get_index_file_path(
                                schema.table_name,
                                column.name,
                                IndexType.ISAM)
        self.step = None

        # asegurarnos de que existe y escribir cabecera
        if not os.path.exists(self.filename):
            open(self.filename, "wb").close()
        with open(self.filename, "r+b") as f:
            f.seek(0)
            f.write(self.HEADER_STRUCT.pack(leaf_factor, index_factor))

    def read_header(self):
        with open(self.filename, "rb") as f:
            lf, ix = self.HEADER_STRUCT.unpack(f.read(self.HEADER_SIZE))
        return lf, ix

    def _fmt_root(self):
        # 'i' + index_factor * (clave FMT + 'ii')
        key_fmt = utils.calculate_column_format(self.column)
        return "i" + (key_fmt + "ii") * self.index_factor

    def _size_root(self):
        return struct.calcsize(self._fmt_root())

    def _offset_root(self):
        return self.HEADER_SIZE

    def read_root_page(self):
        buf = None
        size = self._size_root()
        with open(self.filename, "rb") as f:
            f.seek(self._offset_root())
            buf = f.read(size)
        # desempacar
        hdr = buf[:IndexPage.HSIZE]
        page_num = struct.unpack(IndexPage.HEADER_FMT, hdr)[0]
        records = []
        off = IndexPage.HSIZE
        for _ in range(self.index_factor):
            chunk = buf[off:off+IndexRecord(column=self.column, key=0, left=0, right=0).STRUCT.size]
            records.append(IndexRecord.unpack(self.column, chunk))
            off += IndexRecord( self.column,0,0,0).STRUCT.size
        return IndexPage(page_num, records, self.index_factor)

    def write_root_page(self, page: 'IndexPage'):
        with open(self.filename, "r+b") as f:
            f.seek(self._offset_root())
            f.write(page.pack())

    def _offset_level1(self):
        return self.HEADER_SIZE + self._size_root()

    def read_level1_page(self, page_idx: int) -> 'IndexPage':
        lvl_size = self._size_root()  # mismo formato que root
        off = self._offset_level1() + page_idx * lvl_size
        with open(self.filename, "rb") as f:
            f.seek(off)
            buf = f.read(lvl_size)
        # desempacar idéntico a root
        hdr = buf[:IndexPage.HSIZE]
        page_num = struct.unpack(IndexPage.HEADER_FMT, hdr)[0]
        recs = []
        ptr = IndexPage.HSIZE
        for _ in range(self.index_factor):
            chunk = buf[ptr:ptr+IndexRecord(self.column,0,0,0).STRUCT.size]
            recs.append(IndexRecord.unpack(self.column, chunk))
            ptr += IndexRecord(self.column,0,0,0).STRUCT.size
        return IndexPage(page_num, recs, self.index_factor)

    def write_level1_page(self, page: 'IndexPage'):
        lvl_size = self._size_root()
        off = self._offset_level1() + page.page_num * lvl_size
        with open(self.filename, "r+b") as f:
            f.seek(off)
            f.write(page.pack())

    def _offset_leaves(self):
        # después de ROOT + (index_factor) páginas nivel1
        return self.HEADER_SIZE + self._size_root() * (1 + self.index_factor)

    def _size_leaf(self):
        # cabecera + leaf_factor * (formato clave + i)
        key_fmt = utils.calculate_column_format(self.column)
        sz = struct.calcsize("iii" + key_fmt + "i" * self.leaf_factor)
        return sz

    def read_leaf_page(self, leaf_idx: int) -> 'LeafPage':
        lf, ix = self.read_header()
        sz   = self._size_leaf()
        off  = self._offset_leaves() + leaf_idx * sz
        with open(self.filename, "rb") as f:
            f.seek(off)
            buf = f.read(sz)
        # desempacar
        pn, nxt, nof = struct.unpack(LeafPage.HEADER_FMT, buf[:LeafPage.HSIZE])
        recs = []
        ptr = LeafPage.HSIZE
        for _ in range(self.leaf_factor):
            chunk = buf[ptr:ptr+struct.calcsize(utils.calculate_column_format(self.column)+"i")]
            recs.append(LeafRecord.unpack(self.column, chunk))
            ptr += struct.calcsize(utils.calculate_column_format(self.column)+"i")
        return LeafPage(pn, nxt, nof, recs, self.leaf_factor)

    def write_leaf_page(self, page: 'LeafPage'):
        sz  = self._size_leaf()
        off = self._offset_leaves() + page.page_num * sz
        with open(self.filename, "r+b") as f:
            f.seek(off)
            f.write(page.pack())

    def copy_to_leaf_records(self, rf: RecordFile):
        """
        Lee todos los registros de rf, los ordena por clave primaria,
        y genera EXACTAMENTE p = (i+1)^2 páginas hoja (not_overflow=1),
        con overflow si hace falta, encadenándolas adecuadamente.
        """
        # 1) cargar y ordenar
        leafrecs = []
        pos = 0
        while True:
            rec = rf.read(pos)
            if rec is None:
                break
            key = getattr(rec, "id", rec.values[0])
            leafrecs.append((key, pos))
            pos += 1
        leafrecs.sort(key=lambda x: x[0])

        # 2) parámetros
        l = self.leaf_factor
        i = self.index_factor
        p = (i + 1) ** 2              # número de páginas regulares
        empty_key = utils.get_empty_value(self.column)

        # 3) cálculos de offsets y tamaños
        lf, ix         = self.read_header()
        root_sz        = self._size_root()
        lvl1_sz        = root_sz          # mismo formato que root
        leaves_off     = self._offset_leaves()
        leaf_sz        = self._size_leaf()

        leaf_idx   = 0  # contador global de hojas escritas
        reg_pages  = 0  # cuántas regulares ya
        overflow   = []

        with open(self.filename, "r+b") as f:
            # asegurarnos de reservar hasta la primera hoja
            f.seek(0, os.SEEK_END)
            if f.tell() < leaves_off:
                f.write(b'\x00' * (leaves_off - f.tell()))
            f.seek(leaves_off)

            # 4a) caso: caben en p hojas regulares
            if len(leafrecs) <= p * l:
                idx = 0
                while reg_pages < p and idx < len(leafrecs):
                    remain = len(leafrecs) - idx
                    slots  = p - reg_pages
                    take   = math.ceil(remain / slots)

                    window = leafrecs[idx: idx + take]
                    idx   += take
                    reg_pages += 1

                    # trocear regular vs overflow
                    hoja     = window[:l]
                    overflow = window[l:]

                    # escribir la página regular (not_overflow=1)
                    chunk = [LeafRecord(self.column, k, dp) for k, dp in hoja]
                    while len(chunk) < l:
                        chunk.append(LeafRecord(self.column, empty_key, -1))
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 1, chunk, l))
                    leaf_idx += 1

                    # escribir overflows (not_overflow=0)
                    for j in range(0, len(overflow), l):
                        seg = overflow[j:j+l]
                        chunk = [LeafRecord(self.column, k, dp) for k, dp in seg]
                        while len(chunk) < l:
                            chunk.append(LeafRecord(self.column, empty_key, -1))
                        self.write_leaf_page(LeafPage(leaf_idx, -1, 0, chunk, l))
                        leaf_idx += 1

                # rellenar con hojas vacías hasta p
                while reg_pages < p:
                    chunk = [LeafRecord(self.column, empty_key, -1) for _ in range(l)]
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 1, chunk, l))
                    leaf_idx += 1
                    reg_pages += 1

            # 4b) caso: más de p*l registros → todas overflow=0
            else:
                chunk = []
                for k, dp in leafrecs:
                    chunk.append(LeafRecord(self.column, k, dp))
                    if len(chunk) == l:
                        self.write_leaf_page(LeafPage(leaf_idx, -1, 0, chunk, l))
                        leaf_idx += 1
                        chunk = []
                if chunk:
                    while len(chunk) < l:
                        chunk.append(LeafRecord(self.column, empty_key, -1))
                    self.write_leaf_page(LeafPage(leaf_idx, -1, 0, chunk, l))
                    leaf_idx += 1

        # 5) encadenar todas las hojas (next_page) secuencialmente
        self._link_leaf_pages(leaves_off, leaf_sz, leaf_idx)

    def _link_leaf_pages(self, leaf_off: int, leaf_sz: int, count: int):
        """
        Re-escribe en cada LeafPage su next_page para apuntar a la siguiente.
        """
        empty_key = utils.get_empty_value(self.column)
        with open(self.filename, "r+b") as f:
            for idx in range(count):
                page_start = leaf_off + idx * leaf_sz
                f.seek(page_start)
                # leo header actual
                buf = f.read(LeafPage.HSIZE)
                page_num, _, not_ovf = struct.unpack(LeafPage.HEADER_FMT, buf)
                # calculo siguiente
                next_pg = idx + 1 if idx + 1 < count else -1
                # reescribo sólo la cabecera
                f.seek(page_start)
                # uso pack de un LeafPage vacío para regenerar header
                hdr_only = LeafPage(page_num, next_pg, not_ovf,
                                    [LeafRecord(self.column, empty_key, -1)]*self.leaf_factor,
                                    self.leaf_factor).STRUCT.pack(  # solo HEADER_FMT importa aquí
                    page_num, next_pg, not_ovf, *([0]*(self.leaf_factor*0)) )
                f.write(hdr_only)

    def _build_level1_phase1(self, f, ctx):
        """
        Fase 1: genera páginas de nivel 1 apuntando a hojas reales y sus overflow.
        Al toparse con la primera hoja completamente vacía (todos IDs == empty_value)
        o un 'right' que caiga en hoja vacía, termina y devuelve
        (last_boundary, partial_chunk), donde partial_chunk es la lista de
        IndexRecord que quedó incompleta.
        Actualiza en ctx: ptrs_created, pg, page_idx, seen_count, min_id, max_id, last_valid.
        """
        i = ctx['i']
        p = ctx['p']
        h = ctx['h']
        l = self.leaf_factor
        empty_key = utils.get_empty_value(self.column)

        last_boundary = -1
        partial_chunk = None

        # Mientras queden punteros y hojas reales
        while ctx['ptrs_created'] < p and ctx['pg'] < h:
            chunk = []
            last_c = 1
            last_r = -1

            # Intentamos llenar UNA página índice completa (i registros)
            for _ in range(i):
                if ctx['ptrs_created'] >= p or ctx['pg'] >= h:
                    break

                # (1) calculo de salto c
                rem_leaves = h - ctx['pg']
                rem_ptrs = p - ctx['ptrs_created']
                c = max(1, rem_leaves // rem_ptrs)

                left = ctx['pg']
                ctx['pg'] += c
                last_c = c
                ctx['ptrs_created'] += 1

                # (2) leo hoja 'left'
                lp = self.read_leaf_page(left)
                # detecto “vacía completa” si todos los IDs son empty_key
                if lp is None or all(rec.key == empty_key for rec in lp.records):
                    # corto aquí y paso este chunk parcial a Fase 2
                    partial_chunk = chunk
                    return last_boundary, partial_chunk

                # extraigo sólo IDs válidos para el cálculo de step
                vals = [rec.key for rec in lp.records if rec.key != empty_key]
                if vals:
                    if ctx['seen_count'] == 0:
                        ctx['min_id'] = vals[0]
                    ctx['seen_count'] += len(vals)
                    ctx['last_valid'] = vals[-1]
                    ctx['max_id'] = vals[-1]

                # (3) busco el puntero 'right', saltándome overflow
                cand = ctx['pg'] if ctx['pg'] < h else -1
                while cand != -1:
                    nl = self.read_leaf_page(cand)
                    if nl and nl.records and nl.records[0].key == ctx['last_valid']:
                        cand = nl.next_page
                    else:
                        break
                right = cand if (cand != -1 and cand < h) else -1

                # si 'right' apunta a hoja vacía, también cortamos
                if right != -1:
                    rlp = self.read_leaf_page(right)
                    if rlp is None or all(rec.key == empty_key for rec in rlp.records):
                        partial_chunk = chunk
                        return last_boundary, partial_chunk

                # (4) decido el rec_id: primer key de 'right' o last_valid
                if right != -1:
                    rec_id = self.read_leaf_page(right).records[0].key
                else:
                    rec_id = ctx['last_valid']

                # (5) marco 'left' como página base (not_overflow=1)
                self.write_leaf_page(LeafPage(
                    left, lp.next_page, 1, lp.records, l
                ))

                # (6) añado registro de índice
                chunk.append(IndexRecord(self.column, rec_id, left, right))
                last_r = right

                # preparo siguiente iteración
                if last_r != -1:
                    ctx['pg'] = last_r

            # (7) si no llené la página, devuelvo chunk parcial
            if len(chunk) < i:
                partial_chunk = chunk
                return last_boundary, partial_chunk

            # (8) escribo la página de nivel 1 completa
            self.write_level1_page(IndexPage(ctx['page_idx'], chunk, i))
            ctx['page_idx'] += 1

            # (9) actualizo boundary y salto overflow
            last_boundary = last_r
            if last_r != -1:
                ctx['pg'] = last_r + last_c

        # Si salgo del bucle normalmente, retorno boundary y None
        return last_boundary, None

    def _build_level1_phase2(self, ctx, last_boundary, partial_chunk=None):
        """
        Fase 2: rellena los punteros restantes con hojas “vacías”.
        - ctx['pg'] ya debe estar en last_boundary+1
        - partial_chunk: lista de IndexRecord incompleta de la fase 1, o None
        Actualiza ctx['ptrs_created'], ctx['pg'], ctx['page_idx'].
        """
        i, p, h = ctx['i'], ctx['p'], ctx['h']
        empty_key = utils.get_empty_value(self.column)
        step = ctx['step']
        max_id = ctx['max_id'] or 0

        # (A) arrancar justo después de last_boundary
        if ctx['pg'] <= last_boundary:
            ctx['pg'] = last_boundary + 1

        page2_idx = 0

        # (B) si hay chunk parcial, lo rellenamos primero
        if partial_chunk is not None:
            chunk = partial_chunk
            # base para el primer rec_id: último válido + 1·step
            base_id = max_id + step
            # completamos hasta i registros
            for j in range(len(chunk), i):
                if ctx['ptrs_created'] >= p or ctx['pg'] >= h:
                    break
                left = ctx['pg']
                ctx['pg'] += 1
                ctx['ptrs_created'] += 1
                right = ctx['pg'] if ctx['pg'] < h else -1
                rec_id = base_id + (j - len(partial_chunk)) * step
                chunk.append(IndexRecord(self.column, rec_id, left, right))
            # escribimos la página parcial ya completada
            self.write_level1_page(IndexPage(ctx['page_idx'], chunk, i))
            ctx['page_idx'] += 1
            page2_idx += 1
            # actualizamos max_id al último que acabamos de meter
            max_id = chunk[-1].key

        # (C) ahora construimos páginas completas de Fase 2
        while ctx['ptrs_created'] < p and ctx['pg'] < h and ctx['page_idx'] < (i + 1):
            # saltamos un leaf para no solapar con overflow anterior
            # (siempre avanzamos en +1 aquí)
            # => el primer left de esta página será last_right+1
            #    pero ctx['pg'] ya debe haber quedado correcto tras la fase anterior

            # base_id para esta página: max_id + (page2_idx+1)*step
            base_id = max_id + (page2_idx + 1) * step

            chunk = []
            for j in range(i):
                if ctx['ptrs_created'] >= p or ctx['pg'] >= h:
                    break
                left = ctx['pg']
                ctx['pg'] += 1
                ctx['ptrs_created'] += 1
                right = ctx['pg'] if ctx['pg'] < h else -1
                rec_id = base_id + j * step
                chunk.append(IndexRecord(self.column, rec_id, left, right))

            if not chunk:
                break

            # escribimos página completa
            self.write_level1_page(IndexPage(ctx['page_idx'], chunk, i))
            ctx['page_idx'] += 1
            page2_idx += 1

            # avancemos ctx['pg'] para respetar gap tras overflow
            last_right = chunk[-1].right
            if last_right != -1:
                ctx['pg'] = last_right + 1

            # actualizamos max_id de cara a la próxima página
            max_id = chunk[-1].key

    def build_level1(self):
        """
        Construye t odo el Nivel 1 completo (fases 1 y 2).
        Asume que ya has corrido copy_to_leaf_records() antes.
        """
        # 1) sacar parámetros del header
        lf, ix = self.read_header()  # leaf_factor, index_factor
        i = ix
        p = i * (i + 1)  # punteros totales en nivel1

        # 2) calcular cuántas hojas reales hay
        rec0 = IndexRecord(self.column, 0, 0, 0)
        record_size = rec0.STRUCT.size

        idx_sz = IndexPage.HSIZE + i * record_size
        lr0 = LeafRecord(self.column, 0, 0)
        leaf_record_size = lr0.STRUCT.size

        leaf_sz = LeafPage.HSIZE + lf * leaf_record_size
        total = os.path.getsize(self.filename)
        leaf_off = self.HEADER_SIZE + (1 + i) * idx_sz
        h = (total - leaf_off) // leaf_sz

        # 3) offset donde arrancan las páginas de nivel1 (tras el root)
        level1_off = self.HEADER_SIZE + idx_sz

        # 4) preparar el contexto compartido
        ctx = {
            'i': i,
            'p': p,
            'h': h,
            'ptrs_created': 0,
            'page_idx': 0,
            'pg': 0,
            'min_id': None,
            'max_id': None,
            'seen_count': 0,
            'last_valid': None,
            'step': 0,
        }

        # 5) abrir el archivo y situarnos en nivel1
        with open(self.filename, 'r+b') as f:
            f.seek(level1_off)

            # ––––– FASE 1 –––––
            # devuelve (last_boundary, partial_chunk)
            last_boundary, partial_chunk = self._build_level1_phase1(f, ctx)

            # guardamos cuántas páginas completas escribimos
            ctx['phase1_pages'] = ctx['page_idx']

            # 6) calculo de step para IDs de hojas “vacías”
            if ctx['seen_count'] > 1:
                ctx['step'] = (
                                      ctx['max_id'] - ctx['min_id']
                              ) // (ctx['seen_count'] - 1)
            else:
                ctx['step'] = 1

            # ––––– FASE 2 –––––
            # dejamos ctx['pg'] tal cual (la fase1 ya lo posicionó justo tras last_boundary)
            self._build_level1_phase2(ctx, last_boundary, partial_chunk)

        # 7) almacenamos cuántas páginas de nivel1 creamos
        self.num_level1 = ctx['page_idx']
        self.step = ctx['step']

    def build_root(self):
        """
        Construye la página ROOT (page_num=0) con index_factor registros,
        apuntando a las páginas de nivel1 0..index_factor. El rec_id de cada
        registro es el mínimo de la primera hoja apuntada; si esa hoja está
        “vacía” se utiliza el rec_id de nivel1 menos `self.step`.
        """
        # 1) recupero factores
        lf, ix = self.read_header()  # leaf_factor, index_factor
        i = ix

        records: list[IndexRecord] = []

        # 2) para cada slot en la raíz
        for left in range(i):
            right = left + 1

            # leo la página de nivel1 correspondiente
            lvl1 = self.read_level1_page(right)
            # tomo el primer puntero a hoja de ese nivel1
            first_leaf_pg = lvl1.records[0].left
            leaf = self.read_leaf_page(first_leaf_pg)

            # decido el rec_id:
            if leaf is None or all(r.key == utils.get_empty_value(self.column) for r in leaf.records):
                # hoja “vacía” ⇒ recojo el id del indexrecord de nivel1 y resto step
                base = lvl1.records[0].key
                rec_id = base - self.step
            else:
                # hoja con datos ⇒ mínimo de la hoja
                rec_id = leaf.records[0].id

            records.append(IndexRecord(self.column, rec_id, left, right))

        # 3) empaqueto y escribo la raíz
        root_page = IndexPage(
            page_num=0,
            records=[
                IndexRecord(self.column, rec_id, left, right)
                for (left, right) in ...
            ],
            index_factor=self.index_factor
        )
        self.write_root_page(root_page)

# --------------------------------------------------------------------
# 3) ISAMIndex: lógica del índice
# --------------------------------------------------------------------

class ISAMIndex:
    def __init__(self,
                 schema: TableSchema,
                 column: Column,
                 leaf_factor: int  = 4,
                 index_factor: int = 4):
        self.schema       = schema
        self.column       = column
        self.rf           = RecordFile(schema)
        self.file         = ISAMFile(schema, column, leaf_factor, index_factor)
        self.logger       = logger.CustomLogger(f"ISAMINDEX-{schema.table_name}-{column.name}")
        self.num_level1   = 0
        self.num_leaves   = 0
        self.step         = None

    def build_index(self):
        # 1) copiar todos los records a las hojas
        self.file.copy_to_leaf_records(self.rf)

        # 2) construir lvl1
        self.file.build_level1()

        # 3) construir root
        self.file.build_root()

    def range_search(self, min_key, max_key) -> list[int]:
        """
        Devuelve la lista de datapos de todos los registros con key entre
        min_key y max_key (inclusive), recorriendo hoja tras hoja.
        """
        results = []
        if max_key < min_key:
            return results

        # 1) bajar desde la raíz hasta nivel 1
        root = self.file.read_root_page()
        lvl1_num = root.find_child_ptr(min_key)

        # 2) elegir la página de nivel 1 adecuada
        lvl1 = self.file.read_level1_page(lvl1_num)

        # 3) desde ahí, el puntero a la hoja “base”
        leaf_num = lvl1.find_child_ptr(min_key)
        lp = self.file.read_leaf_page(leaf_num)

        # 4) barrer hoja a hoja hasta pasarnos de max_key
        while lp is not None:
            for rec in lp.records:
                # rec.key y rec.datapos según tu LeafRecord
                if rec.key < min_key:
                    continue
                if rec.key > max_key:
                    return results
                results.append(rec.datapos)
            if lp.next_page == -1:
                break
            lp = self.file.read_leaf_page(lp.next_page)

        return results

    def search(self, key) -> list[int]:
        """
        Búsqueda puntual: sólo devuelve los datapos de los registros con key == key.
        Internamente llama a range_search(key, key).
        """
        return self.range_search(key, key)

    def insert(self, record):
        # persistir en rf.append(record),
        # luego lógica idéntica a tu metodo insert anterior,
        # pero usando self.file.read_leaf_page(), write_leaf_page(), etc.
        ...

    def delete(self, key):
        # replicar tu delete anterior sobre first/last,
        # usando self.file.read_leaf_page(), write_leaf_page(), etc.
        ...

    def getAll(self) -> list[int]:
        # recorres todas las hojas encadenadas y devuelves datapos
        ...

    def clear(self):
        os.remove(self.file.filename)