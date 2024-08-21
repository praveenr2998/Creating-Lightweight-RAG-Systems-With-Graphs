# Creating-Lightweight-RAG-Systems-With-Graphs

## Clone The Repo
```
https://github.com/praveenr2998/Creating-Lightweight-RAG-Systems-With-Graphs.git
```

## Create Virtual Environment
```
cd Creating-Lightweight-RAG-Systems-With-Graphs/
source venv/bin/activate
```

## Load Data
```
cd push_data_to_db
python3 create_nodes.py
python3 create_relationships.py
```

## Run FastAPI app
```
cd fastapi_app
uvicorn app:app --reload
```
