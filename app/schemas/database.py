from typing import List, Dict, Any, Optional
from pydantic import BaseModel

class TableInfo(BaseModel):
    name: str
    columns: List[Dict[str, Any]]

class QueryResult(BaseModel):
    columns: List[str]
    rows: List[Dict[str, Any]]
    count: int

class RowData(BaseModel):
    data: Dict[str, Any]

class TableOperationResponse(BaseModel):
    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None
