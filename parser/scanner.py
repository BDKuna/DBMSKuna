import sys

class Token:
    class Type:
        (
            LPAR, RPAR, SELECT, FROM, WHERE, INSERT, INTO, VALUES, UPDATE, SET, DELETE, 
            CREATE, TABLE, DROP, AND, OR, NOT, AS, ORDER, BY, LIMIT, ID, STAR, BETWEEN,
            EQ, NEQ, LT, GT, LE, GE, COMMA, DOT, SEMICOLON, NUMVAL, FLOATVAL, STRINGVAL,
            BOOLVAL, PRIMARY, KEY, DATATYPE, INDEX, ON, USING, INDEXTYPE, ERR, END, 
            WITHIN, RECTANGLE, CIRCLE, KNN, ASC, DESC, IF, EXISTS
        ) = range(54)

    token_names = [
        "LPAR", "RPAR", "SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES",
        "UPDATE", "SET", "DELETE", "CREATE", "TABLE", "DROP", "AND", "OR", "NOT",
        "AS", "ORDER", "BY", "LIMIT", "ID", "STAR", "BETWEEN", "EQ", "NEQ", "LT",
        "GT", "LE", "GE", "COMMA", "DOT", "SEMICOLON", "NUMVAL", "FLOATVAL", "STRINGVAL",
        "BOOLVAL", "PRIMARY", "KEY", "DATATYPE", "INDEX", "ON", "USING", "INDEXTYPE",
        "ERR", "END", "WITHIN", "RECTANGLE", "CIRCLE", "KNN", "ASC", "DESC", "IF",
        "EXISTS"
    ]

    def __init__(self, token_type, lexema=""):
        self.type = token_type
        self.lexema = lexema

    def __str__(self):
        if self.lexema:
            return f"{Token.token_names[self.type]}({self.lexema})"
        else:
            return Token.token_names[self.type]


class Scanner:
    def __init__(self, input):
        self.input = input + '\0'
        self.first = 0
        self.current = 0
        self.line = 1
        self.pos = 1

    def start_lexema(self) -> None:
        self.first = self.current

    def get_lexema(self) -> str:
        return self.input[self.first:self.current]

    def next_token(self) -> Token:
        state = 0
        self.start_lexema()
        c = self.input[self.current]
        while True:
            if state == 0:
                c = self.input[self.current]
                if c.isspace():
                    if c == '\n':
                        self.line += 1
                        self.pos = 1
                    self.current += 1
                    self.pos += 1
                    self.start_lexema()
                    state = 0
                elif c == '\0':
                    return Token(Token.Type.END)
                elif c == '-':
                    self.current += 1
                    self.pos += 1
                    c = self.input[self.current]
                    if c == '-':
                        self.current += 1
                        self.pos += 1
                        c = self.input[self.current]
                        while c != '\n':
                            self.current += 1
                            self.pos += 1
                            c = self.input[self.current]
                        self.current += 1
                        self.pos += 1
                        self.start_lexema()
                        self.line += 1
                        self.pos = 1
                        state = 0
                    elif c.isdigit():
                        state = 1
                    else:
                        return Token(Token.Type.ERR)
                elif c == '/':
                    self.current += 1
                    self.pos += 1
                    c = self.input[self.current]
                    if c == '*':
                        self.current += 1
                        self.pos += 1
                        c = self.input[self.current]
                        while True:
                            if c == '\n':
                                self.line += 1
                                self.pos = 1
                            if c == '*':
                                self.current += 1
                                self.pos += 1
                                c = self.input[self.current]
                                if c == '/':
                                    break
                            elif c == '\0':
                                break
                            else:
                                self.current += 1
                                self.pos += 1
                                c = self.input[self.current]
                        self.current += 1
                        self.pos += 1
                        self.start_lexema()
                        state = 0
                    else:
                        return Token(Token.Type.ERR)
                elif c == '(':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.LPAR)
                elif c == ')':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.RPAR)
                elif c == '*':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.STAR)
                elif c == '<':
                    self.current += 1
                    self.pos += 1
                    c = self.input[self.current]
                    if c == '=':
                        self.current += 1
                        self.pos += 1
                        return Token(Token.Type.LE)
                    elif c == '>':
                        self.current += 1
                        self.pos += 1
                        return Token(Token.Type.NEQ)
                    else:
                        return Token(Token.Type.LT)
                elif c == '>':
                    self.current += 1
                    self.pos += 1
                    c = self.input[self.current]
                    if c == '=':
                        self.current += 1
                        self.pos += 1
                        return Token(Token.Type.GE)
                    else:
                        return Token(Token.Type.GT)
                elif c == '=':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.EQ)
                elif c == '!':
                    self.current += 1
                    self.pos += 1
                    c = self.input[self.current]
                    if c == '=':
                        self.current += 1
                        self.pos += 1
                        return Token(Token.Type.NEQ)
                    else:
                        return Token(Token.Type.ERR)
                elif c == ',':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.COMMA)
                elif c == '.':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.DOT)
                elif c == ';':
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.SEMICOLON)
                elif c.isdigit():
                    state = 1
                elif c == "'":
                    state = 3
                elif c.isalpha():
                    state = 4
                else:
                    return Token(Token.Type.ERR)

            elif state == 1:
                self.current += 1
                self.pos += 1
                c = self.input[self.current]
                if c.isdigit():
                    pass
                elif c == '.':
                    state = 2
                else:
                    return Token(Token.Type.NUMVAL, self.get_lexema())

            elif state == 2:
                self.current += 1
                self.pos += 1
                c = self.input[self.current]
                if not c.isdigit():
                    return Token(Token.Type.FLOATVAL, self.get_lexema())

            elif state == 3:
                self.current += 1
                self.pos += 1
                c = self.input[self.current]
                if c == "'":
                    self.current += 1
                    self.pos += 1
                    return Token(Token.Type.STRINGVAL, self.get_lexema()[1:-1])
                elif c == '\0':
                    return Token(Token.Type.ERR)

            elif state == 4:
                self.current += 1
                self.pos += 1
                c = self.input[self.current]
                if not (c.isalnum() or c in ["_"]):
                    state = 5

            elif state == 5:
                lexema = self.get_lexema().upper()
                keywords = {
                    "SELECT": Token.Type.SELECT,
                    "FROM": Token.Type.FROM,
                    "WHERE": Token.Type.WHERE,
                    "INSERT": Token.Type.INSERT,
                    "INTO": Token.Type.INTO,
                    "VALUES": Token.Type.VALUES,
                    "UPDATE": Token.Type.UPDATE,
                    "SET": Token.Type.SET,
                    "DELETE": Token.Type.DELETE,
                    "CREATE": Token.Type.CREATE,
                    "TABLE": Token.Type.TABLE,
                    "DROP": Token.Type.DROP,
                    "AND": Token.Type.AND,
                    "OR": Token.Type.OR,
                    "NOT": Token.Type.NOT,
                    "AS": Token.Type.AS,
                    "ORDER": Token.Type.ORDER,
                    "BY": Token.Type.BY,
                    "LIMIT": Token.Type.LIMIT,
                    "BETWEEN": Token.Type.BETWEEN,
                    "TRUE": Token.Type.BOOLVAL,
                    "FALSE": Token.Type.BOOLVAL,
                    "PRIMARY": Token.Type.PRIMARY,
                    "KEY": Token.Type.KEY,
                    "INT": Token.Type.DATATYPE,
                    "FLOAT": Token.Type.DATATYPE,
                    "VARCHAR": Token.Type.DATATYPE,
                    "DATE": Token.Type.DATATYPE,
                    "BOOL": Token.Type.DATATYPE,
                    "POINT": Token.Type.DATATYPE,
                    "INDEX": Token.Type.INDEX,
                    "ON": Token.Type.ON,
                    "USING": Token.Type.USING,
                    "AVL": Token.Type.INDEXTYPE,
                    "ISAM": Token.Type.INDEXTYPE,
                    "HASH": Token.Type.INDEXTYPE,
                    "BTREE": Token.Type.INDEXTYPE,
                    "RTREE": Token.Type.INDEXTYPE,
                    "BRIN": Token.Type.INDEXTYPE,
                    "WITHIN": Token.Type.WITHIN,
                    "RECTANGLE": Token.Type.RECTANGLE,
                    "CIRCLE": Token.Type.CIRCLE,
                    "KNN": Token.Type.KNN,
                    "ASC": Token.Type.ASC,
                    "DESC": Token.Type.DESC,
                    "IF": Token.Type.IF,
                    "EXISTS": Token.Type.EXISTS
                }
                if lexema in keywords:
                    return Token(keywords[lexema], lexema if keywords[lexema] in [Token.Type.BOOLVAL, Token.Type.INDEXTYPE, Token.Type.DATATYPE] else "")
                else:
                    return Token(Token.Type.ID, self.get_lexema())


def test_scanner(scanner : Scanner) -> None:
    while True:
        token = scanner.next_token()
        print(token)
        if token.type == Token.Type.END:
            break
        if token.type == Token.Type.ERR:
            print("Error en scanner - caracter inválido")
            break


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Incorrect number of arguments")
        sys.exit(1)

    scanner = Scanner(sys.argv[1])
    test_scanner(scanner)
