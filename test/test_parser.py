import unittest

import sys, os
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

from parser.parser import print_sql, execute_sql

class MyTestCase(unittest.TestCase):

    def insert_values(self, n: int = 10):
        for i in range(n):
            # Assuming a method to insert values exists
            self.insert_row()   

    def test_something(self):
        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':

    if len(sys.argv) != 2:
        print("Incorrect number of arguments")
        sys.exit(1)
    
    with open(sys.argv[1], 'r') as file:
        sql = file.read()

    print(execute_sql(sql))
