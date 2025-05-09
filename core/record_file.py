import struct
from core.schema import TableSchema, DataType
from core import utils
import logger
import os

class Record:
	def __init__(self, schema: TableSchema, values: list):
		self.schema = schema
		self.values = values
		self.id = values[0]
		self.format = utils.calculate_record_format(schema.columns)
		self.size = struct.calcsize(self.format)
		self.logger = logger.CustomLogger(f"RECORD-{schema.table_name}".upper())

	def debug(self):
		attrs = [
			f"{col.name}: {val}" 
			for col, val in zip(self.schema.columns, self.values)
		]
		debug_msg = f"Record [{', '.join(attrs)}]"
		self.logger.debug(debug_msg)



	def pack(self):
		packed = []
		for col, val in zip(self.schema.columns, self.values):
			if col.data_type == DataType.VARCHAR:
				packed.append(utils.pad_str(val, col.varchar_length))
			else:
				packed.append(val)
		return struct.pack(self.format, *packed)

	@classmethod
	def unpack(cls, schema:TableSchema, raw_bytes):
		format = utils.calculate_record_format(schema.columns)
		values = list(struct.unpack(format, raw_bytes))
		for i, col in enumerate(schema.columns):
			if col.data_type == DataType.VARCHAR:
				values[i] = values[i].decode().strip("\x00")
		return cls(schema, values)

	def __str__(self):
		attrs = [
			f"{col.name}: {val}"
			for col, val in zip(self.schema.columns, self.values)
		]
		return f"Record [{', '.join(attrs)}]"


class FreeListNode:
	def __init__(self, record: Record, next_del=-1):
		self.record = record
		self.next_del = next_del
		self.logger = logger.CustomLogger(f"FREELIST-NODE-{record.schema.table_name}".upper())

	def debug(self):
		self.logger.debug(f"FreeListNode: {self.record.id} -> {self.next_del}")

	@classmethod
	def get_node_size(cls, schema:TableSchema):
		"""Calculate the size of a FreeListNode"""
		return struct.calcsize(utils.calculate_record_format(schema.columns)) + 4

	def pack(self):
		return self.record.pack() + struct.pack("i", self.next_del)

	@classmethod
	def unpack(cls, schema:TableSchema, raw_bytes):
		record = Record.unpack(schema, raw_bytes[:-4])
		# Unpack the last 4 bytes as the next_del
		# struct.calcsize("i") = 4 bytes
		next_del = struct.unpack("i", raw_bytes[-4:])[0]
		return cls(record, next_del)




class RecordFile:
	"""HeapFile with Free List for deleted records"""
	HEADER_FORMAT = "i"  # 4 bytes for the header (pointer to the first free record)
	HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
	HEADER:int # persisted header, the first free record

	def __init__(self, schema: TableSchema):
		self.filename = utils.get_record_file_path(schema.table_name)
		self.schema = schema
		self.node_size = FreeListNode.get_node_size(schema)
		self.logger = logger.CustomLogger(f"RECORDFILE-{schema.table_name}".upper())
		#self.logger.logger.setLevel(logging.WARNING)
		
		if not os.path.exists(self.filename):
			self.logger.fileNotFound(self.filename)
			open(self.filename, "wb").close() # create empty file
		self._initialize_file()

	# ----- Private methods -----

	def _initialize_file(self):
		with open(self.filename, "wb+") as file:
			header = file.read(self.HEADER_SIZE)
			if not header:
				self.logger.fileIsEmpty(self.filename)
				self.HEADER = -1
				file.write(struct.pack(self.HEADER_FORMAT, self.HEADER))
			else:
				self.HEADER = struct.unpack(self.HEADER_FORMAT, header)[0]

	def _get_header(self):
		return self.HEADER

	def _set_header(self, header:int):
		self.HEADER = header
		with open(self.filename, "r+b") as file:
			file.seek(0)
			file.write(struct.pack(self.HEADER_FORMAT, self.HEADER))
			self.logger.writingHeader(self.filename, header)

	def _append_node(self, record: Record) -> int:
		"""Append a record to the end of the file and return its position"""
		with open(self.filename, "ab") as file:
			offset = file.tell() // self.node_size
			node = FreeListNode(record)
			file.write(node.pack())
			self.logger.writingRecord(self.filename, offset, record.values[0], node.next_del)  # should be the first an id
			return offset

	def _read_node(self, pos: int) -> FreeListNode:
		with open(self.filename, "rb") as file:
			self.logger.readingNode(self.filename, pos)
			file.seek(self.HEADER_SIZE + pos * self.node_size)
			data = file.read(self.node_size)
			if not data:
				self.logger.invalidPosition(self.filename, pos)
				raise Exception(f"Invalid record position: {pos}")
			node = FreeListNode.unpack(self.schema, data)
			node.debug()
			return node

	def _patch_node(self, pos: int, node: FreeListNode):
		with open(self.filename, "wb+") as file:
			offset = self.HEADER_SIZE + pos * self.node_size
			if offset > os.path.getsize(self.filename):
				self.logger.invalidPosition(self.filename, pos)
				raise Exception(f"Invalid record position: {pos}")
			file.seek(offset)
			file.write(node.pack())
			self.logger.writingRecord(self.filename, pos, node.record.values[0])

	# ----- Public methods -----

	def append(self, record: Record) -> int:
		"""Append a record to the file and return its position"""
		if self._get_header() == -1:
			return self._append_node(record)
		tdel_pos = self._get_header() # position of the record to delete
		del_node = self._read_node(tdel_pos) # read the node to delete
		self._set_header(del_node.next_del) # update the header to the next free record
		self._patch_node(tdel_pos,FreeListNode(record)) # append the new record to the deleted node

	
	def read(self, pos: int) -> Record:
		"""Read a record from the file at the given position"""
		node = self._read_node(pos)
		if node.next_del == -1:
			return node.record
		else:
			self.logger.notFoundRecord(self.filename, pos)
			return None # todo: handle this case
	
	def delete(self, pos: int)-> Record:
		"""Delete a record at the given position and add it to the free list"""
		tdel_node = self._read_node(pos) #to delete node
		tdel_node.next_del = self._get_header()
		self._set_header(pos)
		self._patch_node(pos, tdel_node)
		return tdel_node.record

	def clear(self):
		self.logger.info("Cleaning data, removing files")
		os.remove(self.filename)

	def __str__(self):
		print(f"RecordFile: {self.filename}")
		print(f"Header: {self.HEADER}")
		print(f"Node size: {self.node_size}")
		i = 0
		while True:
			try:
				node = self._read_node(i)
				print(f"Node {i}: {node.record.values} -> {node.next_del}")
				i += 1
			except Exception as e:
				break
		return ""

