import sys, os
root_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if root_path not in sys.path:
    sys.path.append(root_path)
import subprocess

from fastapi import FastAPI
from fastapi import HTTPException, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
import time

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
    print(q)
    try:
        start = time.time()
        result, message = parser.execute_sql(q.query)
        end = time.time()
    except RuntimeError as e:
        end = time.time()
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
        'message': message,
        'execution_time': end - start
    }


@app.post("/upload_img/")
async def upload_img(file: UploadFile = File(...), table: str = Form(...), column: str = Form(...)):
    """Receive a JPG image and store it under the data/img directory."""
    print(f"Uploading image file to table: {table}, column: {column}...")
    if not file.filename.lower().endswith(".jpg"):
        raise HTTPException(status_code=400, detail="Only JPG files are allowed")

    upload_dir = os.path.join(root_path, "data", "img")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.filename)

    with open(file_path, "wb") as f:
        contents = await file.read()
        f.write(contents)

    return {
        "filename": file.filename,
        "saved_to": file_path,
        "table": table,
        "column": column
    }

@app.post("/upload_aud/")
async def upload_aud(file: UploadFile = File(...), table: str = Form(...), column: str = Form(...)):
    """Receive an MP3 audio file and store it under the data/mp3 directory."""
    print(f"Uploading audio file to table: {table}, column: {column}...")
    if not file.filename.lower().endswith(".mp3"):
        raise HTTPException(status_code=400, detail="Only MP3 files are allowed")

    upload_dir = os.path.join(root_path, "data", "mp3")
    os.makedirs(upload_dir, exist_ok=True)
    file_path = os.path.join(upload_dir, file.filename)

    with open(file_path, "wb") as f:
        contents = await file.read()
        f.write(contents)

    return {"filename": file.filename, "saved_to": file_path}
