import os, sys
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
from parser.scanner import Token, Scanner
from core.conditionschema import BinaryOp, Condition, ConditionColumn, ConditionValue, NotCondition, BinaryCondition, BetweenCondition, BooleanColumn
from core.schema import TableSchema, DataType, IndexType, SelectSchema, DeleteSchema, ConditionSchema, Column
from core.dbmanager import DBManager

class Stmt:
    def __init__(self):
        pass

class SelectStmt(Stmt):
    def __init__(self, table_name : str = None, condition : Condition = None, all : bool = False, column_list : list[str] = None, order_by : str = None, asc : bool = True, limit : int = None):
        super().__init__()
        self.table_name = table_name
        self.condition = condition
        self.all = all
        self.column_list = column_list if column_list else []
        self.order_by = order_by
        self.asc = asc
        self.limit = limit

    def add_column(self, column_name : str) -> None:
        self.column_list.append(column_name)

class InsertStmt(Stmt):
    def __init__(self, table_name : str = None, column_list : list[str] = None, value_list : list = None):
        super().__init__()
        self.table_name = table_name
        self.column_list = column_list if column_list else []
        self.value_list = value_list if value_list else []

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
    def __init__(self, column_name : str = None, data_type : DataType = None, is_primary_key : bool = False, index_type : IndexType = IndexType.NONE, varchar_limit : int = 0):
        self.column_name = column_name
        self.data_type = data_type
        self.is_primary_key = is_primary_key
        self.index_type = index_type
        self.varchar_limit = varchar_limit

class CreateTableStmt(Stmt):
    def __init__(self, table_name : str = None, column_def_list : list[ColumnDefinition] = None, if_not_exists: bool = False):
        super().__init__()
        self.table_name = table_name
        self.column_def_list = column_def_list if column_def_list else []
        self.if_not_exists = if_not_exists
    
    def add_column_definition(self, column_def : ColumnDefinition = None) -> None:
        self.column_def_list.append(column_def)

# <drop-table-stmt> ::= "DROP" "TABLE" <table-name>
class DropTableStmt(Stmt):
    def __init__(self, table_name : str = None, if_exists: bool = False):
        super().__init__()
        self.table_name = table_name
        self.if_exists = if_exists

# <create-index-stmt> ::= "CREATE" "INDEX" <index-name> "ON" <table-name> [ "USING" <index-type> ] "(" <column-list> ")"
class CreateIndexStmt(Stmt):
    def __init__(self, index_name : str = None, table_name : str = None, index_type : IndexType = None, column_list : list[str] = None):
        super().__init__()
        self.index_name = index_name
        self.table_name = table_name
        self.index_type = index_type
        self.column_list = column_list if column_list else []

    def add_column(self, column_name : str) -> None:
        self.column_list.append(column_name)

# <drop-index-stmt> ::= "DROP" "INDEX" <index-name> [ "ON" <table-name> ]
class DropIndexStmt(Stmt):
    def __init__(self, index_name : str = None, table_name : str = None):
        super().__init__()
        self.index_name = index_name
        self.table_name = table_name

class SQL:
    def __init__(self, stmt_list : list[Stmt] = None):
        self.stmt_list = stmt_list if stmt_list else []

    def add_stmt(self, stmt : Stmt) -> None:
        self.stmt_list.append(stmt)


class ParseError(Exception):
    def __init__(self, error : str, line : int, pos : int, token : Token):
        self.error = f"Parse error: {error} (at line {line} position {pos} with token {token})"
        print(self.error)
        super().__init__(self.error)

class Parser:
    def __init__(self, scanner : Scanner):
        self.scanner = scanner
        self.current : Token = None
        self.previous : Token = None

    def error(self, error : str):
        raise ParseError(error, self.scanner.line, self.scanner.pos, self.current)

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
                self.error(f"unrecognized character: {self.current.lexema}")

    def is_at_end(self) -> bool:
        return self.current.type == Token.Type.END

    def str_into_type(self, value, token : Token):
        if token.type == Token.Type.NUMVAL:
            return int(value)
        elif token.type == Token.Type.FLOATVAL:
            return float(value)
        elif token.type == Token.Type.STRINGVAL:
            return value
        elif token.type == Token.Type.BOOLVAL:
            if token.lexema == "TRUE":
                return True
            elif token.lexema == "FALSE":
                return False

    def parse(self) -> SQL:
        try:
            self.current = self.scanner.next_token()
            return self.parse_sql()
        except ParseError as e:
            raise e

    # <sql> ::= <statement_list>
    # <statement_list> ::= <statement> ";" { <statement> ";" }
    def parse_sql(self) -> SQL:
        sql = SQL()
        sql.add_stmt(self.parse_stmt())
        while(self.match(Token.Type.SEMICOLON) and self.current.type != Token.Type.END):
            sql.add_stmt(self.parse_stmt())
        if self.current.type != Token.Type.END:
            self.error("unexpected items after statement")
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
                self.error("expected TABLE or INDEX keyword after CREATE keyword")
        elif self.match(Token.Type.DROP):
            if self.match(Token.Type.TABLE):
                return self.parse_drop_table_stmt()
            elif self.match(Token.Type.INDEX):
                return self.parse_drop_index_stmt()
            else:
                self.error("expected TABLE or INDEX keyword after DROP keyword")
        elif self.match(Token.Type.INSERT):
            return self.parse_insert_stmt()
        elif self.match(Token.Type.DELETE):
            return self.parse_delete_stmt()
        elif self.match(Token.Type.SELECT):
            return self.parse_select_stmt()
        else:
            self.error("unexpected start of an instruction")

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
                    self.error("expected column name after comma")
        else:
            self.error("expected '*' or column name after SELECT keyword")
        if not self.match(Token.Type.FROM):
            self.error("expected FROM clause in SELECT statement")
        if not self.match(Token.Type.ID):
            self.error("expected table name after FROM keyword")
        select_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.WHERE):
            select_stmt.condition = self.parse_or_condition()
        if self.match(Token.Type.ORDER):
            if not self.match(Token.Type.BY):
                self.error("expected BY keyword after ORDER keyword")
            if not self.match(Token.Type.ID):
                self.error("expected column name in ORDER BY clause")
            select_stmt.order_by = self.previous.lexema
            if self.match(Token.Type.ASC):
                select_stmt.asc = True
            elif self.match(Token.Type.DESC):
                select_stmt.asc = False
        if self.match(Token.Type.LIMIT):
            if not self.match(Token.Type.NUMVAL):
                self.error("expected valid int value after LIMIT keyword")
            select_stmt.limit = self.str_into_type(self.previous.lexema, self.previous)
        return select_stmt

    # <create-table-stmt> ::= "CREATE" "TABLE" <table-name> "(" <column-def-list> ")"
    # <column-def-list> ::= <column-def> { "," <column-def> }
    
    def parse_create_table_stmt(self) -> CreateTableStmt:
        create_table_stmt = CreateTableStmt()
        if self.match(Token.Type.IF):
            if not self.match(Token.Type.NOT):
                self.error("expected NOT keyword after IF keyword")
            if not self.match(Token.Type.EXISTS):
                self.error("expected EXISTS keyword after NOT keyword")
            create_table_stmt.if_not_exists = True
        if not self.match(Token.Type.ID):
            self.error("expected table name after CREATE TABLE keyword")
        create_table_stmt.table_name = self.previous.lexema
        if not self.match(Token.Type.LPAR):
            self.error("expected '(' after table name")
        create_table_stmt.add_column_definition(self.parse_column_def())
        while self.match(Token.Type.COMMA):
            create_table_stmt.add_column_definition(self.parse_column_def())
        if not self.match(Token.Type.RPAR):
            self.error("expected ')' after column definitions")
        return create_table_stmt

    # <column-def> ::= <column-name> <data-type> [ "PRIMARY" "KEY" ] [ "INDEX" <index-type> ]
    def parse_column_def(self) -> ColumnDefinition:
        column_definition = ColumnDefinition()
        if not self.match(Token.Type.ID):
            self.error("expected column name in column definition")
        column_definition.column_name = self.previous.lexema
        if not self.match(Token.Type.DATATYPE):
            self.error("expected valid data type after column name")
        match self.previous.lexema:
            case "INT":
                column_definition.data_type = DataType.INT
            case "FLOAT":
                column_definition.data_type = DataType.FLOAT
            case "VARCHAR":
                column_definition.data_type = DataType.VARCHAR
                if not self.match(Token.Type.LPAR):
                    self.error("expected '(' after VARCHAR keyword")
                if not self.match(Token.Type.NUMVAL):
                    self.error("expected number after '('")
                column_definition.varchar_limit = int(self.previous.lexema)
                if not self.match(Token.Type.RPAR):
                    self.error("expected ')' after number")
            case "DATE":
                column_definition.data_type = DataType.DATE
            case "BOOL":
                column_definition.data_type = DataType.BOOL
            case "POINT":
                column_definition.data_type = DataType.POINT
            case _:
                self.error("unknown data type")
        if self.match(Token.Type.PRIMARY):
            if not self.match(Token.Type.KEY):
                self.error("expected KEY keyword after PRIMARY keyword")
            column_definition.is_primary_key = True
        if self.match(Token.Type.INDEX):
            if not self.match(Token.Type.INDEXTYPE):
                self.error("expected valid index type in column definition")
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
                case "BRIN":
                    column_definition.index_type = IndexType.BRIN
                case _:
                    self.error("unknown index type")
        else:
            column_definition.index_type = IndexType.NONE
        return column_definition
                

    # <drop-table-stmt> ::= "DROP" "TABLE" <table-name>
    def parse_drop_table_stmt(self) -> DropTableStmt:
        drop_table_stmt = DropTableStmt()
        if self.match(Token.Type.IF):
            if not self.match(Token.Type.EXISTS):
                self.error("expected EXISTS keyword after IF keyword")
            drop_table_stmt.if_exists = True
        if not self.match(Token.Type.ID):
            self.error("expected table name after DROP TABLE keyword")
        drop_table_stmt.table_name = self.previous.lexema
        return drop_table_stmt

    def match_values(self) -> bool:
        return self.match(Token.Type.NUMVAL) or self.match(Token.Type.FLOATVAL) or self.match(Token.Type.STRINGVAL) or self.match(Token.Type.BOOLVAL)

    # <insert-stmt> ::= "INSERT" "INTO" <table-name> [ "(" <column-list> ")" ] "VALUES" "(" <value-list> ")"
    # <column-list> ::= <column-name> { "," <column-name> }
    # <value-list> ::= <value> { "," <value> }
    def parse_insert_stmt(self) -> InsertStmt:
        insert_stmt = InsertStmt()
        if not self.match(Token.Type.INTO):
            self.error("expected INTO keyword after INSERT keyword")
        if not self.match(Token.Type.ID):
            self.error("expected table name after INSERT INTO keyword")
        insert_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.LPAR):
            if not self.match(Token.Type.ID):
                self.error("expected column name after '('")
            insert_stmt.add_column(self.previous.lexema)
            while self.match(Token.Type.COMMA):
                if not self.match(Token.Type.ID):
                    self.error("expected column name after comma")
                insert_stmt.add_column(self.previous.lexema)
            if not self.match(Token.Type.RPAR):
                self.error("expected ')' after column names")
        if not self.match(Token.Type.VALUES):
            self.error("expected VALUES clause in INSERT statement")
        if not self.match(Token.Type.LPAR):
            self.error("expected '(' after VALUES keyword")
        if self.match(Token.Type.LPAR): # POINT
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected a valid float value por x coordinate on POINT declaration")
            x = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after x coordiante")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected a valid float value por y coordinate on POINT declaration")
            y = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.RPAR):
                self.error("expected ')' after y coordinate")
            insert_stmt.add_value((x, y))
        else:
            if not self.match_values():
                self.error("expected value after '('")
            insert_stmt.add_value(self.str_into_type(self.previous.lexema, self.previous))
        while self.match(Token.Type.COMMA):
            if self.match(Token.Type.LPAR): # POINT
                if not self.match(Token.Type.FLOATVAL):
                    self.error("expected a valid float value por x coordinate on POINT declaration")
                x = self.str_into_type(self.previous.lexema, self.previous)
                if not self.match(Token.Type.COMMA):
                    self.error("expected comma after x coordiante")
                if not self.match(Token.Type.FLOATVAL):
                    self.error("expected a valid float value por y coordinate on POINT declaration")
                y = self.str_into_type(self.previous.lexema, self.previous)
                if not self.match(Token.Type.RPAR):
                    self.error("expected ')' after y coordinate")
                insert_stmt.add_value((x, y))
            else:
                if not self.match_values():
                    self.error("expected value after comma")
                insert_stmt.add_value(self.str_into_type(self.previous.lexema, self.previous))
        if not self.match(Token.Type.RPAR):
            self.error("expected ')' after values")
        return insert_stmt

    # <delete-stmt> ::= "DELETE" "FROM" <table-name> [ "WHERE" <condition> ]
    def parse_delete_stmt(self) -> DeleteStmt:
        delete_stmt = DeleteStmt()
        if not self.match(Token.Type.FROM):
            self.error("expected FROM keyword after DELETE keyword")
        if not self.match(Token.Type.ID):
            self.error("expected table name after DELETE FROM keyword")
        delete_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.WHERE):
            delete_stmt.condition = self.parse_or_condition()
        return delete_stmt

    # <create-index-stmt> ::= "CREATE" "INDEX" <index-name> "ON" <table-name> [ "USING" <index-type> ] "(" <column-list> ")"
    # <column-list> ::= <column-name> { "," <column-name> }
    def parse_create_index_stmt(self) -> CreateIndexStmt:
        create_index_stmt = CreateIndexStmt()
        if not self.match(Token.Type.ID):
            self.error("expected index name after CREATE INDEX keyword")
        create_index_stmt.index_name = self.previous.lexema
        if not self.match(Token.Type.ON):
            self.error("expected ON keyword after index name")
        if not self.match(Token.Type.ID):
            self.error("expected table name after ON keyword")
        create_index_stmt.table_name = self.previous.lexema
        if self.match(Token.Type.USING):
            if not self.match(Token.Type.INDEXTYPE):
                self.error("expected valid index type after USING keyword")
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
                case "BRIN":
                    create_index_stmt.index_type = IndexType.BRIN
                case _:
                    self.error("unknown index type")
        if not self.match(Token.Type.LPAR):
            self.error("expected '(' after table name or index type")
        if not self.match(Token.Type.ID):
            self.error("expected column name after '('")
        create_index_stmt.add_column(self.previous.lexema)
        while self.match(Token.Type.COMMA):
            if not self.match(Token.Type.ID):
                self.error("expected column name after comma")
            create_index_stmt.add_column(self.previous.lexema)
        if not self.match(Token.Type.RPAR):
            self.error("expected ')' after column names")
        return create_index_stmt

    # <drop-index-stmt> ::= "DROP" "INDEX" <index-name> [ "ON" <table-name> ]
    def parse_drop_index_stmt(self) -> DropIndexStmt:
        drop_index_stmt = DropIndexStmt()
        if not self.match(Token.Type.ID):
            self.error("expected index name after DROP INDEX keyword")
        drop_index_stmt.index_name = self.previous.lexema
        if not self.match(Token.Type.ON):
            self.error("expected ON keyword after index name")
        if not self.match(Token.Type.ID):
            self.error("expected table name after ON keyword")
        drop_index_stmt.table_name = self.previous.lexema
        return drop_index_stmt
    
    # <or-condition> ::= <and-condition> { "OR" <and-condition> }
    def parse_or_condition(self) -> Condition:
        left = self.parse_and_condition()
        while self.match(Token.Type.OR):
            right = self.parse_and_condition()
            left = BinaryCondition(left, BinaryOp.OR, right)
        return left

    # <and-condition> ::= <not-condition> { "AND" <not-condition> }
    def parse_and_condition(self) -> Condition:
        left = self.parse_not_condition()
        while self.match(Token.Type.AND):
            right = self.parse_not_condition()
            left = BinaryCondition(left, BinaryOp.AND, right)
        return left

    # <not-condition> ::= [ "NOT" ] <predicate>
    def parse_not_condition(self) -> Condition:
        if(self.match(Token.Type.NOT)):
            return NotCondition(self.parse_predicate())
        return self.parse_predicate()

    # <predicate> ::= <simple-condition> | "(" <condition> ")"
    def parse_predicate(self) -> Condition:
        if(self.match(Token.Type.LPAR)):
            condition = self.parse_or_condition()
            if not self.match(Token.Type.RPAR):
                self.error("expected ')' to close a condition")
            return condition
        return self.parse_simple_condition()

    # <simple-condition> ::= <column-name> <operator> <value> | <boolean-column-name> | <column-name> "BETWEEN" <value> "AND" <value>
    def parse_simple_condition(self) -> Condition:
        if not self.match(Token.Type.ID):
            self.error("expected column name in condition")
        column_name = self.previous.lexema
        if self.match(Token.Type.BETWEEN):
            between_condition = BetweenCondition()
            between_condition.left = ConditionColumn(column_name)
            if not self.match_values():
                self.error("expected a value after BETWEEN keyword")
            between_condition.mid = ConditionValue(self.str_into_type(self.previous.lexema, self.previous)) # TODO depende del tipo (en utils)
            if not self.match(Token.Type.AND):
                self.error("expected AND keyword after value in BETWEEN clause")
            if not self.match_values():
                self.error("expected a value after AND keyword y BETWEEN clause")
            between_condition.right = ConditionValue(self.str_into_type(self.previous.lexema, self.previous))
            return between_condition
        simple_condition = BinaryCondition()
        simple_condition.left = ConditionColumn(column_name)
        if not (self.match(Token.Type.LT) or self.match(Token.Type.GT) or self.match(Token.Type.LE) or self.match(Token.Type.GE) or self.match(Token.Type.EQ) or self.match(Token.Type.NEQ) or self.match(Token.Type.WITHIN) or self.match(Token.Type.KNN)):
            return BooleanColumn(column_name)
        
        match self.previous.type:
            case Token.Type.LT:
                simple_condition.op = BinaryOp.LT
            case Token.Type.GT:
                simple_condition.op = BinaryOp.GT
            case Token.Type.LE:
                simple_condition.op = BinaryOp.LE
            case Token.Type.GE:
                simple_condition.op = BinaryOp.GE
            case Token.Type.EQ:
                simple_condition.op = BinaryOp.EQ
            case Token.Type.NEQ:
                simple_condition.op = BinaryOp.NEQ
            case Token.Type.WITHIN:
                if self.match(Token.Type.RECTANGLE):
                    simple_condition.op = BinaryOp.WR
                elif self.match(Token.Type.CIRCLE):
                    simple_condition.op = BinaryOp.WC
                else:
                    self.error("expected RECTANGLE or CIRCLE after WITHIN")
            case Token.Type.KNN:
                simple_condition.op = BinaryOp.KNN
            case _:
                self.error("unknown conditional operator")
        if simple_condition.op == BinaryOp.WR:
            if not self.match(Token.Type.LPAR):
                self.error("expected '(' after WITHIN RECTANGLE operation")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for min x coordinate")
            x_min = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after min x coordinate")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for min x coordinate")
            y_min = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after min x coordinate")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for min x coordinate")
            x_max = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after min x coordinate")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for min x coordinate")
            y_max = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.RPAR):
                self.error("expected ')' after max y coordinate")
            simple_condition.right = ConditionValue((x_min, y_min, x_max, y_max))
        elif simple_condition.op == BinaryOp.WC:
            if not self.match(Token.Type.LPAR):
                self.error("expected '(' after WITHIN CIRCLE operation")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for x coordinate")
            x = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after x coordinate")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for y coordinate")
            y = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after y coordinate")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for radius")
            radius = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.RPAR):
                self.error("expected ')' after radius")
            simple_condition.right = ConditionValue((x, y, radius))
        elif simple_condition.op == BinaryOp.KNN:
            if not self.match(Token.Type.LPAR):
                self.error("expected '(' after KNN operation")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for x coordinate")
            x = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after x coordinate")
            if not self.match(Token.Type.FLOATVAL):
                self.error("expected valid float value for y coordinate")
            y = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.COMMA):
                self.error("expected comma after y coordinate")
            if not self.match(Token.Type.NUMVAL):
                self.error("expected valid int value for k value")
            k = self.str_into_type(self.previous.lexema, self.previous)
            if not self.match(Token.Type.RPAR):
                self.error("expected ')' after k value")
            simple_condition.right = ConditionValue((x, y, k))
        else:
            if self.match(Token.Type.LPAR): # POINT
                if not self.match(Token.Type.FLOATVAL):
                    self.error("expected a valid float value por x coordinate on POINT declaration")
                x = self.str_into_type(self.previous.lexema, self.previous)
                if not self.match(Token.Type.COMMA):
                    self.error("expected comma after x coordiante")
                if not self.match(Token.Type.FLOATVAL):
                    self.error("expected a valid float value por y coordinate on POINT declaration")
                y = self.str_into_type(self.previous.lexema, self.previous)
                if not self.match(Token.Type.RPAR):
                    self.error("expected ')' after y coordinate")
                simple_condition.right = ConditionValue((x, y))
            else:
                if not self.match_values():
                    self.error("expected a value after conditional operator")
                simple_condition.right = ConditionValue(self.str_into_type(self.previous.lexema, self.previous))
        return simple_condition


class PrintError(Exception):
    def __init__(self, error : str):
        self.error = f"Printer error: {error}"
        super().__init__(self.error)

class Printer:
    def __init__(self):
        self.indent = 0

    def error(self, error : str):
        raise PrintError(error)

    def print_line(self, line : str):
        print(f"{' '*self.indent}{line}")

    def print(self, sql : SQL):
        try:
            self.print_sql(sql)
        except PrintError as e:
            print(e.error)

    def print_sql(self, sql : SQL):
        if not sql:
            self.error("Invalid sql")
        for stmt in sql.stmt_list:
            self.print_stmt(stmt)

    def print_stmt(self, stmt : Stmt):
        stmt_type = type(stmt)
        if stmt_type == SelectStmt:
            self.print_select_stmt(stmt)
        elif stmt_type == CreateTableStmt:
            self.print_create_table_stmt(stmt)
        elif stmt_type == DropTableStmt:
            self.print_drop_table_stmt(stmt)
        elif stmt_type == InsertStmt:
            self.print_insert_stmt(stmt)
        elif stmt_type == DeleteStmt:
            self.print_delete_stmt(stmt)
        elif stmt_type == CreateIndexStmt:
            self.print_create_index_stmt(stmt)
        elif stmt_type == DropIndexStmt:
            self.print_drop_index_stmt(stmt)
        else:
            self.error("unknown statement type")

    def print_select_stmt(self, stmt : SelectStmt):
        self.print_line("SELECT statement:")
        self.indent += 2
        self.print_line("-> Table name:")
        self.indent += 2
        self.print_line(f"-> {stmt.table_name}")
        self.indent -= 2
        self.print_line("-> Selected columns:")
        self.indent += 2
        if stmt.all:
            self.print_line("-> All (*)")
        else:
            self.print_line(f"-> {', '.join(str(column) for column in stmt.column_list)}")
        self.indent -= 2
        self.print_condition_main(stmt.condition)
        self.indent -= 2
    
    def binary_condition_to_str(self, condition : BinaryCondition):
        if condition.op not in [BinaryOp.AND, BinaryOp.OR]:
            op = None
            match condition.op:
                case BinaryOp.EQ:
                    op = "="
                case BinaryOp.NEQ:
                    op = "!="
                case BinaryOp.LT:
                    op = "<"
                case BinaryOp.GT:
                    op = ">"
                case BinaryOp.LE:
                    op = "<="
                case BinaryOp.GE:
                    op = ">="
                case BinaryOp.WR:
                    op = "WITHIN RECTANGLE"
                case BinaryOp.WC:
                    op = "WITHIN CIRCLE"
                case BinaryOp.KNN:
                    op = "KNN"
                case _:
                    self.error("unknown operation")
            return f"{self.condition_column_to_str(condition.left)} {op} {self.value_to_str(condition.right)}"
        else:
            return "AND" if condition.op == BinaryOp.AND else "OR"

    def condition_to_str(self, condition : Condition):
        condition_type = type(condition)
        if condition_type == BinaryCondition:
            return self.binary_condition_to_str(condition)
        elif condition_type == BooleanColumn:
            return condition.column_name
        elif condition_type == BetweenCondition:
            return f"{condition.left.column_name} BETWEEN {self.value_to_str(condition.mid)} AND {self.value_to_str(condition.right)}"
        elif condition_type == NotCondition:
            return "NOT"
        elif condition_type == ConditionValue:
            return self.value_to_str(condition)
        elif condition_type == ConditionColumn:
            return condition.column_name    
        else:
            self.error("unknown condition type")

    def value_to_str(self, val):
        return str(val.value) if isinstance(val, ConditionValue) else str(val)

    def condition_column_to_str(self, condition : ConditionColumn):
        return condition.column_name

    def print_condition_tree(self, condition, prefix="", is_last=True):
        connector = "└─ " if is_last else "├─ "
        self.print_line(prefix + connector + self.condition_to_str(condition))

        children = []
        if isinstance(condition, BinaryCondition) and condition.op in {BinaryOp.AND, BinaryOp.OR}:
            children = [condition.left, condition.right]
        elif isinstance(condition, NotCondition):
            children = [condition.condition]

        new_prefix = prefix + ("   " if is_last else "│  ")

        for i, child in enumerate(children):
            self.print_condition_tree(child, new_prefix, i == len(children) - 1)

    def print_condition_main(self, condition : Condition):
        self.print_line("-> Condition:")
        self.indent += 2
        if not condition:
            self.print_line("-> No condition")
        else:
            self.print_condition_tree(condition)
        self.indent -= 2

    def print_create_table_stmt(self, stmt : CreateTableStmt):
        self.print_line("CREATE TABLE statement:")
        self.indent += 2
        self.print_line("-> Table name:")
        self.indent += 2
        self.print_line(f"-> {stmt.table_name}")
        self.indent -= 2
        self.print_line("-> Columns:")
        self.indent += 2
        for index, column_def in enumerate(stmt.column_def_list):
            self.print_line(f"-> Column {index + 1}")
            self.indent += 2
            self.print_column_def(column_def)
            self.indent -= 2
        self.indent -= 4

    def print_column_def(self, column_def : ColumnDefinition):
        self.print_line("-> Column name:")
        self.indent += 2
        self.print_line(f"-> {column_def.column_name}")
        self.indent -= 2
        self.print_line("-> Is primary key?:")
        self.indent += 2
        self.print_line(f"-> {'Yes' if column_def.is_primary_key else 'No'}")
        self.indent -= 2
        self.print_line("-> Data type:")
        self.indent += 2
        match column_def.data_type:
            case DataType.INT:
                self.print_line("-> INT")
            case DataType.FLOAT:
                self.print_line("-> FLOAT")
            case DataType.VARCHAR:
                self.print_line("-> VARCHAR")
            case DataType.DATE:
                self.print_line("-> DATE")
            case DataType.BOOL:
                self.print_line("-> BOOL")
            case DataType.POINT:
                self.print_line("-> POINT")
        self.indent -= 2
        if column_def.data_type == DataType.VARCHAR:
            self.print_line("-> Varchar limit:")
            self.indent += 2
            self.print_line(f"-> {column_def.varchar_limit}")
            self.indent -= 2
        self.print_line("-> Index type:")
        self.indent += 2
        match column_def.index_type:
            case IndexType.AVL:
                self.print_line(f"-> AVL")
            case IndexType.ISAM:
                self.print_line(f"-> ISAM")
            case IndexType.HASH:
                self.print_line(f"-> HASH")
            case IndexType.BTREE:
                self.print_line(f"-> BTREE")
            case IndexType.RTREE:
                self.print_line(f"-> RTREE")
            case IndexType.BRIN:
                self.print_line(f"-> BRIN")
            case IndexType.NONE:
                self.print_line(f"-> NONE")
        self.indent -= 2

    def print_drop_table_stmt(self, stmt : DropTableStmt):
        self.print_line("DROP TABLE statement:")
        self.indent += 2
        self.print_line("-> Table name:")
        self.indent += 2
        self.print_line(stmt.table_name)
        self.indent -= 4

    def print_insert_stmt(self, stmt : InsertStmt):
        self.print_line("INSERT statement:")
        self.indent += 2
        self.print_line("-> Into table:")
        self.indent += 2
        self.print_line(f"-> {stmt.table_name}")
        self.indent -= 2
        if stmt.column_list:
            self.print_line("-> Into columns:")
            self.indent += 2
            self.print_line(f"-> {', '.join(str(column) for column in stmt.column_list)}")
            self.indent -= 2
        self.print_line("-> Values:")
        self.indent += 2
        self.print_line(f"-> {', '.join(str(value) for value in stmt.value_list)}")
        self.indent -= 4

    def print_delete_stmt(self, stmt : DeleteStmt):
        self.print_line("DELETE statement:")
        self.indent += 2
        self.print_line("-> From table:")
        self.indent += 2
        self.print_line(f"-> {stmt.table_name}")
        self.indent -= 2
        self.print_condition_main(stmt.condition)
        self.indent -= 2

    def print_create_index_stmt(self, stmt : CreateIndexStmt):
        self.print_line("CREATE INDEX statement:")
        self.indent += 2
        self.print_line("-> Index name:")
        self.indent += 2
        self.print_line(f"-> {stmt.index_name}")
        self.indent -= 2
        self.print_line("-> On table:")
        self.indent += 2
        self.print_line(f"-> {stmt.table_name}")
        self.indent -= 2
        self.print_line("-> Index type:")
        self.indent += 2
        match stmt.index_type:
            case IndexType.AVL:
                self.print_line(f"-> AVL")
            case IndexType.ISAM:
                self.print_line(f"-> ISAM")
            case IndexType.HASH:
                self.print_line(f"-> HASH")
            case IndexType.BTREE:
                self.print_line(f"-> BTREE")
            case IndexType.RTREE:
                self.print_line(f"-> RTREE")
            case IndexType.BRIN:
                self.print_line(f"-> BRIN")
        self.indent -= 2
        self.print_line("-> On columns:")
        self.indent += 2
        self.print_line(f"-> {', '.join(str(column) for column in stmt.column_list)}")
        self.indent -= 4

    def print_drop_index_stmt(self, stmt : DropIndexStmt):
        self.print_line("DROP INDEX statement:")
        self.indent += 2
        self.print_line("-> Index name:")
        self.indent += 2
        self.print_line(f"-> {stmt.index_name}")
        self.indent -= 2
        if stmt.table_name:
            self.print_line("-> On table:")
            self.indent += 2
            self.print_line(f"-> {stmt.table_name}")
            self.indent -= 2
        self.indent -= 2


class RuntimeError(Exception):
    def __init__(self, error : str):
        self.error = f"Runtime error: {error}"
        print(self.error)
        super().__init__(self.error)


class Interpreter:
    def __init__(self):
        self.dbmanager = DBManager()

    def error(self, error : str):
        raise RuntimeError(error)
    
    def interpret(self, sql : SQL):
        try:
            return self.interpret_sql(sql)
        except RuntimeError as e:
            raise e

    def interpret_sql(self, sql : SQL):
        if not sql:
            self.error("Invalid SQL")
        for stmt in sql.stmt_list:
            result = self.interpret_stmt(stmt)
        return result

    def interpret_stmt(self, stmt : Stmt):
        stmt_type = type(stmt)
        if stmt_type == SelectStmt:
            return self.interpret_select_stmt(stmt), "Selection successful"
        elif stmt_type == CreateTableStmt:
            self.interpret_create_table_stmt(stmt)
            return None, "Table created successfully"
        elif stmt_type == DropTableStmt:
            self.interpret_drop_table_stmt(stmt)
            return None, "Table dropped successfully"
        elif stmt_type == InsertStmt:
            self.interpret_insert_stmt(stmt)
            return None, "Insertion successful"
        elif stmt_type == DeleteStmt:
            self.interpret_delete_stmt(stmt)
            return None, "Deletion successful"
        elif stmt_type == CreateIndexStmt:
            self.interpret_create_index_stmt(stmt)
            return None, "Index created successfully"
        elif stmt_type == DropIndexStmt:
            self.interpret_drop_index_stmt(stmt)
            return None, "Index dropped successfully"
        else:
            self.error("unknown statement type")

    def interpret_select_stmt(self, stmt : SelectStmt):
        select_schema = SelectSchema(stmt.table_name, ConditionSchema(stmt.condition), stmt.all, stmt.column_list, stmt.order_by, stmt.asc, stmt.limit)
        return self.dbmanager.select(select_schema)

    def interpret_create_table_stmt(self, stmt : CreateTableStmt):
        column_list = [Column(column_def.column_name, column_def.data_type, column_def.is_primary_key, column_def.index_type, column_def.varchar_limit) for column_def in stmt.column_def_list]
        table_schema = TableSchema(stmt.table_name, column_list)
        self.dbmanager.create_table(table_schema, stmt.if_not_exists)

    def interpret_drop_table_stmt(self, stmt : DropTableStmt):
        self.dbmanager.drop_table(stmt.table_name, stmt.if_exists)

    def interpret_insert_stmt(self, stmt : InsertStmt):
        self.dbmanager.insert(stmt.table_name, stmt.value_list, stmt.column_list)

    def interpret_delete_stmt(self, stmt : DeleteStmt):
        delete_schema = DeleteSchema(stmt.table_name, ConditionSchema(stmt.condition))
        self.dbmanager.delete(delete_schema)

    def interpret_create_index_stmt(self, stmt : CreateIndexStmt):
        self.dbmanager.create_index(stmt.table_name, stmt.index_name, stmt.column_list, stmt.index_type)

    def interpret_drop_index_stmt(self, stmt : DropIndexStmt):
        self.dbmanager.drop_index(stmt.table_name, stmt.index_name)


def execute_sql(sql:str):
    scanner = Scanner(sql)
    try:
        parser = Parser(scanner)
        sql_parse = parser.parse()
    except ParseError as e:
        return None, str(e)

    try:
        interpreter = Interpreter()
        return interpreter.interpret(sql_parse)  # (result, message)
    except RuntimeError as e:
        return None, str(e)

def print_sql(sql: str):
    scanner = Scanner(sql)
    try:
        parser = Parser(scanner)
        sql_parse = parser.parse()
    except ParseError as e:
        return None, str(e)

    try:
        printer = Printer()
        print(printer.print(sql_parse))
    except RuntimeError as e:
        return None, str(e)