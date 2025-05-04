import struct
from schema import TableSchema, DataType
from utils import calculate_record_format, pad_str
import logger
import os

class Record:
    def __init__(self, schema: TableSchema, values: list):
        self.schema = schema
        self.values = values
        self.format = calculate_record_format(schema.columns)
        self.size = struct.calcsize(self.format)

    def pack(self):
        packed = []
        for col, val in zip(self.schema.columns, self.values):
            if col.data_type == DataType.VARCHAR:
                packed.append(pad_str(val, col.varchar_length))
            else:
                packed.append(val)
        return struct.pack(self.format, *packed)

    @classmethod
    def unpack(cls, schema, raw_bytes):
        format = calculate_record_format(schema.columns)
        values = list(struct.unpack(format, raw_bytes))
        for i, col in enumerate(schema.columns):
            if col.data_type == DataType.VARCHAR:
                values[i] = values[i].decode().strip("\x00")
        return cls(schema, values)


class RecordFile:
	def __init__(self, filename, schema: TableSchema):
		self.filename = filename
		self.schema = schema
		self.record_size = struct.calcsize(calculate_record_format(schema.columns))
		self.logger = logger.CustomLogger("RECORDFILE")
		#self.logger.logger.setLevel(logging.WARNING)
		
		if not os.path.exists(self.filename):
			self.logger.fileNotFound(self.filename)
			self.initialize_file(filename) # if archive not exists
		
	def initialize_file(self, filename):
		with open(filename, "wb") as file:
			pass
	
	def append(self, record: Record) -> int:
		with open(self.filename, "ab") as file:
			offset = file.tell() // Record.size
			file.write(record.pack())
			self.logger.writingRecord(self.filename, offset, record.schema.id)
			return offset
	
	def read(self, pos: int) -> Record:
		with open(self.filename, "rb") as file:
			file.seek(pos * Record.size)
			data = file.read(Record.size)
			if not data:
				self.logger.invalidPosition(self.filename, pos)
				raise Exception(f"Invalid record position: {pos}")
			record = Record.unpack(data)
			self.logger.foundRecord(self.filename, pos, record.id)
			return record
	
	def delete(self, pos: int):
		pass
