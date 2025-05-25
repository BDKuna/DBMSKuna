import sys, os
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
import subprocess

from fastapi import FastAPI
from fastapi import HTTPException
from fastapi.middleware.cors import CORSMiddleware

try:
    from parser import parser
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "bitarray"])
    from parser import parser

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
    try:
        result, message = parser.execute_sql(q.query)
    except RuntimeError as e:
        result, message = None, str(e)
        pass
        #raise HTTPException(status_code=400, detail=str(e))
    
    resultPagination = {
        'columns': [],
        'records': []
    }
    if result is not None:
        resultPagination = {
            'columns': result['columns'],
            'records': result['records'][q.offset : q.offset + q.limit]
        }
        total = len(result['records'])
    else:
        total = 0
    return {
        'data': resultPagination,
        'total': total,
        'message': message
    }
