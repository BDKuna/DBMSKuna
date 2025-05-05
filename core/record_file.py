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


class RecordFile:
	def __init__(self, schema: TableSchema):
		self.filename = utils.get_record_file_path(schema.table_name)
		self.schema = schema
		self.record_size = struct.calcsize(utils.calculate_record_format(schema.columns))
		self.logger = logger.CustomLogger(f"RECORDFILE-{schema.table_name}".upper())
		#self.logger.logger.setLevel(logging.WARNING)
		
		if not os.path.exists(self.filename):
			self.logger.fileNotFound(self.filename)
			self.initialize_file(self.filename) # if archive not exists
		
	def initialize_file(self, filename):
		with open(filename, "wb") as file:
			pass
	
	def append(self, record: Record) -> int:
		with open(self.filename, "ab") as file:
			offset = file.tell() // self.record_size
			file.write(record.pack())
			self.logger.writingRecord(self.filename, offset, record.values[0]) # should be the first an id
			return offset
	
	def read(self, pos: int) -> Record:
		with open(self.filename, "rb") as file:
			file.seek(pos * self.record_size)
			data = file.read(self.record_size)
			if not data:
				self.logger.invalidPosition(self.filename, pos)
				raise Exception(f"Invalid record position: {pos}")
			record:Record = Record.unpack(data)
			self.logger.foundRecord(self.filename, pos, record.id)
			return record
	
	def delete(self, pos: int):
		pass
