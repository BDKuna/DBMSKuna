import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.schema import TableSchema, Column
from core import utils
from core.record_file import RecordFile

class NoIndex:
	def __init__(self, schema:TableSchema, column:Column):
		self.column = column
		self.schema = schema
		for pos, i in enumerate(schema.columns):
			if i == column:
				self.value_pos = pos
				break
				
	def insert(self, pos : int, val : any):
		pass
	
	def getAll(self) -> list[int]:
		pass
	
	def search(self, key) -> list[int]:
		record_file = RecordFile(self.schema)
		pos = 0
		max_pos = record_file.max_id()
		res = []
		
		while pos < max_pos:
			record = record_file.read(pos)
			if record.values[self.value_pos] == key:
				res.append(pos)
			pos += 1
		return res

	def rangeSearch(self, ini, end) -> list[int]:
		if(ini == None):
			ini = utils.get_min_value(self.column)
		if(end == None):
			end = utils.get_max_value(self.column)
		
		record_file = RecordFile(self.schema)
		pos = 0
		max_pos = record_file.max_id()
		res = []
		
		while pos < max_pos:
			record = record_file.read(pos)
			if record.values[self.value_pos] >= ini and record.values[self.value_pos] <= end:
				res.append(pos)
			pos += 1
		return res
	
	def clear(self):
		pass