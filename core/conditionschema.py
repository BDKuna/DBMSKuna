from enum import Enum, auto

class BinaryOp(Enum):
    AND = auto()
    OR = auto()
    EQ = auto()
    NEQ = auto()
    LT = auto()
    GT = auto()
    LE = auto()
    GE = auto()
    WC = auto()
    WR = auto()
    KNN = auto()

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

class BooleanColumn(Condition):
    def __init__(self, column_name : str = None):
        super().__init__()
        self.column_name = column_name

class ConditionColumn(Condition):
    def __init__(self, column_name : str = None):
        super().__init__()
        self.column_name = column_name

class ConditionValue(Condition):
    def __init__(self, value = None):
        super().__init__()
        self.value = value

class ConditionSchema:
    def __init__(self, condition : Condition = None):
        self.condition = condition