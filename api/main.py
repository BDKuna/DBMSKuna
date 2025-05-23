import sys, os
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

from fastapi import FastAPI
from parser import parser
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pydantic import BaseModel

class Query(BaseModel):
    query: str
    limit: int
    offset: int

@app.post("/sql/")
def query(q: Query):
    result, message = parser.execute_sql(q.query)
    
    resultPagination = {
        'columns': [],
        'records': []
    }
    if result is not None:
        resultPagination = {
            'columns': result['columns'],
            'records': result['records'][q.offset : q.offset + q.limit]
        }
    return {
        'data': resultPagination,
        'total': len(result['records']),
        'message': message
    }
