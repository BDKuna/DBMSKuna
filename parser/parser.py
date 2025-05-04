from scanner import Token, Scanner
from enum import Enum, auto
from sqlbuilder import DataType, IndexType
import sys

class BinaryOp(Enum):
    AND = auto()
    OR = auto()
    EQ = auto()
    NEQ = auto()
    LT = auto()
    GT = auto()
    LE = auto()
    GE = auto()

class Condition:
    def __init__(self):
        pass

class BinaryCondition(Condition):
    def __init__(self, left : Condition = None, op : BinaryOp = None, right : Condition = None):
        super().__init__()
        self.left = left
        self.op = op
        self.right = right

class BetweenCondition(Condition):
    def __init__(self, left : Condition = None, mid : Condition = None, right : Condition = None):
        super().__init__()
        self.left = left
        self.mid = mid
        self.right = right

class NotCondition(Condition):
    def __init__(self, condition : Condition = None):
        super().__init__()
        self.condition = condition

class ConditionColumn(Condition):
    def __init__(self, column_name : str = None):
        super().__init__()
        self.column_name = column_name

class ConditionValue(Condition):
    def __init__(self, value = None):
        super().__init__()
        self.value = value

class Stmt:
    def __init__(self):
        pass

class SelectStmt(Stmt):
    def __init__(self, table_name : str = None, condition : Condition = None, all : bool = False, column_list : list[str] = []):
        super().__init__()
        self.table_name = table_name
        self.condition = condition
        self.all = all
        self.column_list = column_list

    def add_column(self, column_name : str) -> None:
        self.column_list.append(column_name)

class InsertStmt(Stmt):
    def __init__(self, table_name : str = None, column_list : list[str] = [], value_list : list = []):
        super().__init__()
        self.table_name = table_name
        self.column_list = column_list
        self.value_list = value_list

    def add_column(self, column_name : str) -> None:
        self.column_list.append(column_name)

    def add_value(self, value) -> None:
        self.value_list.append(value)

class DeleteStmt(Stmt):
    def __init__(self, table_name : str = None, condition : Condition = None):
        super().__init__()
        self.table_name = table_name
        self.condition = condition

# <column-def> ::= <column-name> <data-type> [ "PRIMARY" "KEY" ] [ "INDEX" <index-type> ]
class ColumnDefinition():
    def __init__(self, column_name : str = None, data_type : DataType = None, is_primary_key : bool = False, index_type : IndexType = IndexType.SEQ, varchar_limit : int = 0):
        self.column_name = column_name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
        self.index_type = index_type
        self.varchar_limit = varchar_limit

class CreateTableStmt(Stmt):
    def __init__(self, table_name : str = None, column_def_list : list[ColumnDefinition] = []):
        super().__init__()
        self.table_name = table_name
        self.column_def_list = column_def_list
    
    def add_column_definition(self, column_def : ColumnDefinition = None) -> None:
        self.column_def_list.append(column_def)

# <drop-table-stmt> ::= "DROP" "TABLE" <table-name>
class DropTableStmt(Stmt):
    def __init__(self, table_name : str = None):
        super().__init__()
        self.table_name = table_name

# <create-index-stmt> ::= "CREATE" "INDEX" <index-name> "ON" <table-name> [ "USING" <index-type> ] "(" <column-list> ")"
class CreateIndexStmt(Stmt):
    def __init__(self, index_name : str = None, table_name : str = None, index_type : IndexType = IndexType.BTREE, column_list : list[str] = []):
        super().__init__()
        self.index_name = index_name
        self.table_name = table_name
        self.index_type = index_type
        self.column_list = column_list

    def add_column(self, column_name : str) -> None:
        self.column_list.append(column_name)

# <drop-index-stmt> ::= "DROP" "INDEX" <index-name> [ "ON" <table-name> ]
class DropIndexStmt(Stmt):
    def __init__(self, index_name : str = None, table_name : str = None):
        super().__init__()
        self.index_name = index_name
        self.table_name = table_name

class SQL:
    def __init__(self, stmt_list : list[Stmt] = []):
        self.stmt_list = stmt_list

    def add_stmt(self, stmt : Stmt) -> None:
        self.stmt_list.append(stmt)


class ParseError(Exception):
    def __init__(self, error : str):
        self.error = f"Parse error: {error}"
        super().__init__(self.error)

class Parser:
    def __init__(self, scanner : Scanner):
        self.scanner = scanner
        self.current : Token = None
        self.previous : Token = None

    def parse_error(self, error : str):
        raise ParseError(error)

    def match(self, type : Token.Type) -> bool:
        if self.check(type):
            self.advance()
            return True
        else:
            return False

    def check(self, type : Token.Type) -> bool:
        if self.is_at_end():
            return False
        else:
            return self.current.type == type

    def advance(self) -> None:
        if not self.is_at_end():
            temp = self.current
            self.current = self.scanner.next_token()
            self.previous = temp
            if self.check(Token.Type.ERR):
                self.parse_error(f"unrecognized character: {self.current.lexema}")

    def is_at_end(self) -> bool:
        return self.current.type == Token.Type.END

    def parse(self) -> SQL:
        try:
            self.current = self.scanner.next_token()
            return self.parse_sql()
        except ParseError as e:
            print(e.error)

    # <sql> ::= <statement_list>
    # <statement_list> ::= <statement> ";" { <statement> ";" }
    def parse_sql(self) -> SQL:
        sql = SQL()
        sql.add_stmt(self.parse_stmt())
        while(self.match(Token.Type.SEMICOLON) and self.current.type != Token.Type.END):
            sql.add_stmt(self.parse_stmt())
        if self.current.type != Token.Type.END:
            self.parse_error("unexpected items after statement")
        return sql
    
    # <statement> ::= <select-stmt>
    #         | <create-table-stmt>
    #         | <drop-table-stmt>
    #         | <insert-stmt>
    #         | <delete-stmt>
    #         | <create-index-stmt>
    #         | <drop-index-stmt>
    def parse_stmt(self) -> Stmt:
        if self.match(Token.Type.SELECT):
            return self.parse_select_stmt()
        elif self.match(Token.Type.CREATE):
            if self.match(Token.Type.TABLE):
                return self.parse_create_table_stmt()
            elif self.match(Token.Type.INDEX):
                return self.parse_create_index_stmt()
            else:
                self.parse_error("expected TABLE or INDEX keyword after CREATE keyword")
        elif self.match(Token.Type.DROP):
            if self.match(Token.Type.TABLE):
                return self.parse_drop_table_stmt()
            elif self.match(Token.Type.INDEX):
                return self.parse_drop_index_stmt()
            else:
                self.parse_error("expected TABLE or INDEX keyword after DROP keyword")
        elif self.match(Token.Type.INSERT):
            return self.parse_insert_stmt()
        elif self.match(Token.Type.DELETE):
            return self.parse_delete_stmt()
        elif self.match(Token.Type.SELECT):
            return self.parse_select_stmt()
        else:
            self.parse_error("unexpected start of an instruction")

    # <select-stmt> ::= "SELECT" <select-list> "FROM" <table-name> [ "WHERE" <condition> ]
    # <select-list> ::= "*" | <column-name> { "," <column-name> }
    def parse_select_stmt(self) -> SelectStmt:
        select_stmt = SelectStmt()
        if self.match(Token.Type.STAR):
            select_stmt.all = True
        elif self.match(Token.Type.ID):
            select_stmt.add_column(self.previous.lexema)
            while(self.match(Token.Type.COMMA)):
                if self.match(Token.Type.ID):
                    select_stmt.add_column(self.previous.lexema)
                else:
                    self.parse_error("expected column name after comma")
        else:
            self.parse_error("expected '*' or column name after SELECT keyword")
        if not self.match(Token.Type.FROM):
            self.parse_error("expected FROM clause in SELECT statement")
        if not self.match(Token.Type.ID):
            self.parse_error("expected table name after FROM keyword")
        select_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.WHERE):
            select_stmt.condition = self.parse_or_condition()
        return select_stmt

    # <create-table-stmt> ::= "CREATE" "TABLE" <table-name> "(" <column-def-list> ")"
    # <column-def-list> ::= <column-def> { "," <column-def> }
    
    def parse_create_table_stmt(self) -> CreateTableStmt:
        create_table_stmt = CreateTableStmt()
        if not self.match(Token.Type.ID):
            self.parse_error("expected table name after CREATE TABLE keyword")
        create_table_stmt.table_name = self.previous.lexema
        if not self.match(Token.Type.LPAR):
            self.parse_error("expected '(' after table name")
        create_table_stmt.add_column_definition(self.parse_column_def())
        while self.match(Token.Type.COMMA):
            create_table_stmt.add_column_definition(self.parse_column_def())
        if not self.match(Token.Type.RPAR):
            self.parse_error("expected ')' after column definitions")
        return create_table_stmt

    # <column-def> ::= <column-name> <data-type> [ "PRIMARY" "KEY" ] [ "INDEX" <index-type> ]
    def parse_column_def(self) -> ColumnDefinition:
        column_definition = ColumnDefinition()
        if not self.match(Token.Type.ID):
            self.parse_error("expected column name in column definition")
        column_definition.column_name = self.previous.lexema
        if not self.match(Token.Type.DATATYPE):
            self.parse_error("expected valid data type after column name")
        match self.previous.lexema:
            case "INT":
                column_definition.data_type = DataType.INT
            case "FLOAT":
                column_definition.data_type = DataType.FLOAT
            case "VARCHAR":
                column_definition.data_type = DataType.VARCHAR
                if not self.match(Token.Type.LPAR):
                    self.parse_error("expected '(' after VARCHAR keyword")
                if not self.match(Token.Type.NUMVAL):
                    self.parse_error("expected number after '('")
                column_definition.varchar_limit = self.previous.lexema
                if not self.match(Token.Type.RPAR):
                    self.parse_error("expected ')' after number")
            case "DATE":
                column_definition.data_type = DataType.DATE
            case "BOOL":
                column_definition.data_type = DataType.BOOL
            case _:
                self.parse_error("unknown data type")
        if self.match(Token.Type.PRIMARY):
            if not self.match(Token.Type.KEY):
                self.parse_error("expected KEY keyword after PRIMARY keyword")
            column_definition.is_primary_key = True
        if self.match(Token.Type.INDEX):
            if not self.match(Token.Type.INDEXTYPE):
                self.parse_error("expected valid index type in column definition")
            match self.previous.lexema:
                case "AVL":
                    column_definition.index_type = IndexType.AVL
                case "ISAM":
                    column_definition.index_type = IndexType.ISAM
                case "HASH":
                    column_definition.index_type = IndexType.HASH
                case "BTREE":
                    column_definition.index_type = IndexType.BTREE
                case "RTREE":
                    column_definition.index_type = IndexType.RTREE
                case "SEQ":
                    column_definition.index_type = IndexType.SEQ
                case _:
                    self.parse_error("unknown index type")
        else:
            column_definition.index_type = IndexType.SEQ
        return column_definition
                

    # <drop-table-stmt> ::= "DROP" "TABLE" <table-name>
    def parse_drop_table_stmt(self) -> DropTableStmt:
        drop_table_stmt = DropTableStmt()
        if not self.match(Token.Type.ID):
            self.parse_error("expected table name after DROP TABLE keyword")
        drop_table_stmt.table_name = self.previous.lexema
        return drop_table_stmt

    # <insert-stmt> ::= "INSERT" "INTO" <table-name> [ "(" <column-list> ")" ] "VALUES" "(" <value-list> ")"
    # <column-list> ::= <column-name> { "," <column-name> }
    # <value-list> ::= <value> { "," <value> }
    def parse_insert_stmt(self) -> InsertStmt:
        insert_stmt = InsertStmt()
        if not self.match(Token.Type.INTO):
            self.parse_error("expected INTO keyword after INSERT keyword")
        if not self.match(Token.Type.ID):
            self.parse_error("expected table name after INSERT INTO keyword")
        insert_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.LPAR):
            if not self.match(Token.Type.ID):
                self.parse_error("expected column name after '('")
            insert_stmt.add_column(self.previous.lexema)
            while self.match(Token.Type.COMMA):
                if not self.match(Token.Type.ID):
                    self.parse_error("expected column name after comma")
                insert_stmt.add_column(self.previous.lexema)
            if not self.match(Token.Type.RPAR):
                self.parse_error("expected ')' after column names")
        if not self.match(Token.Type.VALUES):
            self.parse_error("expected VALUES clause in INSERT statement")
        if not self.match(Token.Type.LPAR):
            self.parse_error("expected '(' after VALUES keyword")
        if not self.match(Token.Type.NUMVAL) or self.match(Token.Type.FLOATVAL) or self.match(Token.Type.STRINGVAL) or self.match(Token.Type.BOOLVAL):
            self.parse_error("expected value after '('")
        insert_stmt.add_value(self.previous.lexema)
        while self.match(Token.Type.COMMA):
            if not self.match(Token.Type.NUMVAL) or self.match(Token.Type.FLOATVAL) or self.match(Token.Type.STRINGVAL) or self.match(Token.Type.BOOLVAL):
                self.parse_error("expected value after comma")
            insert_stmt.add_value(self.previous.lexema)
        if not self.match(Token.Type.RPAR):
            self.parse_error("expected ')' after values")
        return insert_stmt

    # <delete-stmt> ::= "DELETE" "FROM" <table-name> [ "WHERE" <condition> ]
    def parse_delete_stmt(self) -> DeleteStmt:
        delete_stmt = DeleteStmt()
        if not self.match(Token.Type.FROM):
            self.parse_error("expected FROM keyword after DELETE keyword")
        if not self.match(Token.Type.ID):
            self.parse_error("expected table name after DELETE FROM keyword")
        delete_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.WHERE):
            delete_stmt.condition = self.parse_or_condition()
        return delete_stmt

    # <create-index-stmt> ::= "CREATE" "INDEX" <index-name> "ON" <table-name> [ "USING" <index-type> ] "(" <column-list> ")"
    # <column-list> ::= <column-name> { "," <column-name> }
    def parse_create_index_stmt(self) -> CreateIndexStmt:
        create_index_stmt = CreateIndexStmt()
        if not self.match(Token.Type.ID):
            self.parse_error("expected index name after CREATE INDEX keyword")
        create_index_stmt.index_name = self.previous.lexema
        if not self.match(Token.Type.ON):
            self.parse_error("expected ON keyword after index name")
        if not self.match(Token.Type.ID):
            self.parse_error("expected table name after ON keyword")
        create_index_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.USING):
            if not self.match(Token.Type.INDEXTYPE):
                self.parse_error("expected valid index type after USING keyword")
            match self.previous.lexema:
                case "AVL":
                    create_index_stmt.index_type = IndexType.AVL
                case "ISAM":
                    create_index_stmt.index_type = IndexType.ISAM
                case "HASH":
                    create_index_stmt.index_type = IndexType.HASH
                case "BTREE":
                    create_index_stmt.index_type = IndexType.BTREE
                case "RTREE":
                    create_index_stmt.index_type = IndexType.RTREE
                case "SEQ":
                    create_index_stmt.index_type = IndexType.SEQ
                case _:
                    self.parse_error("unkonwn index type")
        if not self.match(Token.Type.LPAR):
            self.parse_error("expected '(' after table name or index type")
        if not self.match(Token.Type.ID):
            self.parse_error("expected column name after '('")
        create_index_stmt.add_column(self.previous.lexema)
        while self.match(Token.Type.COMMA):
            if not self.match(Token.Type.ID):
                self.parse_error("expected column name after comma")
            create_index_stmt.add_column(self.previous.lexema)
        if not self.match(Token.Type.RPAR):
            self.parse_error("expected ')' after column names")
        return create_index_stmt

    # <drop-index-stmt> ::= "DROP" "INDEX" <index-name> [ "ON" <table-name> ]
    def parse_drop_index_stmt(self) -> DropIndexStmt:
        drop_index_stmt = DropIndexStmt()
        if not self.match(Token.Type.ID):
            self.parse_error("expected index name after DROP INDEX keyword")
        drop_index_stmt.index_name = self.previous.lexema
        if self.match(Token.Type.ON):
            if not self.match(Token.Type.ID):
                self.parse_error("expected table name after ON keyword")
            drop_index_stmt.table_name = self.previous.lexema
        return drop_index_stmt
    
    # <or-condition> ::= <and-condition> { "OR" <and-condition> }
    def parse_or_condition(self) -> Condition:
        pass

    # <and-condition> ::= <not-condition> { "AND" <not-condition> }
    def parse_and_condition(self) -> Condition:
        pass

    # <not-condition> ::= [ "NOT" ] <predicate>
    def parse_not_condition(self) -> NotCondition:
        pass

    # <predicate> ::= <simple-condition> | "(" <condition> ")"
    def parse_predicate(self) -> Condition:
        pass

    # <simple-condition> ::= <column-name> <operator> <value> | <column-name> "BETWEEN" <value> "AND" <value>
    def parse_simple_condition(self) -> Condition:
        pass

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect number of arguments")
        sys.exit(1)

    scanner = Scanner(sys.argv[1])
    parser = Parser(scanner)
    parser.parse()