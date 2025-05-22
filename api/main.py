import sys, os
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)

from fastapi import FastAPI
from core.dbmanager import DBManager
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

    return {"Hello": "World"}

"""
@app.post("/sql/select_all/")
def select_all(q: Query):
    db = DBManager()
    result = db.select_all(db.get_table_schema(q.query))
    resultPagination = {
        'columns': result['columns'],
        'records': result['records'][q.offset : q.offset + q.limit]
    }
    return {
        'data': resultPagination,
        'total': len(result['records'])
    }
"""