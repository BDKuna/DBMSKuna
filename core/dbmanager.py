from schema import DataType, TableSchema

class DBManager:
    def __init__(self):
        pass

    def create_table(self, table_schema : TableSchema):
        format = ""
        for column in table_schema.columns:
            match column.data_type:
                case DataType.INT:
                    format += "i"
                case DataType.FLOAT:
                    format += "f"
                case DataType.VARCHAR:
                    format += f"{column.varchar_length}s"
                case DataType.DATE:
                    format += "10s"
                case DataType.BOOL:
                    format += "b"
                case _:
                    pass