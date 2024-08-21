from fastapi import FastAPI
from pydantic import BaseModel
from query_engine import GraphQueryEngine


# Pydantic model
class QueryRequest(BaseModel):
    query: str


app = FastAPI()


@app.post("/process-query/")
async def process_query(request: QueryRequest):
    query_engine = GraphQueryEngine()
    cypher_queries = query_engine.get_response(request.query)
    cypher_queries = query_engine.populate_embedding_in_query(request.query, cypher_queries)
    fetched_data = query_engine.fetch_data(cypher_queries)
    response = query_engine.get_final_response(request.query, fetched_data)
    return {"response": response}

