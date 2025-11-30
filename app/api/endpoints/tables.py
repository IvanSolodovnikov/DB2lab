from fastapi import APIRouter, Depends, HTTPException, status, Query
from typing import List, Dict, Any, Optional
from sqlalchemy.orm import Session

from app.db.database import DatabaseManager
from app.models.base import get_db as get_sqlite_db
from app.core.security import get_current_active_user
from app.models.user import User
from app.schemas.database import TableInfo, QueryResult, RowData, TableOperationResponse
from app.core.config import get_settings

router = APIRouter()
settings = get_settings()
db_manager = DatabaseManager(settings.DATABASE_URL)

@router.get("/tables", response_model=List[str])
async def list_tables(
    current_user: User = Depends(get_current_active_user)
) -> List[str]:
    """Get list of all tables in the database"""
    return db_manager.get_tables()

@router.get("/tables/{table_name}", response_model=TableInfo)
async def get_table_info(
    table_name: str,
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """Get information about a specific table"""
    try:
        columns = db_manager.get_table_columns(table_name)
        return {"name": table_name, "columns": columns}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Table '{table_name}' not found or error: {str(e)}"
        )

@router.get("/tables/{table_name}/rows", response_model=QueryResult)
async def get_table_rows(
    table_name: str,
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """Get paginated rows from a table"""
    try:
        return db_manager.get_table_data(table_name, skip=skip, limit=limit)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error fetching data: {str(e)}"
        )

@router.post("/tables/{table_name}/rows", response_model=TableOperationResponse)
async def add_table_row(
    table_name: str,
    row_data: Dict[str, Any],
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """Add a new row to the table"""
    result = db_manager.insert_row(table_name, row_data)
    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result['message']
        )
    return result

@router.put("/tables/{table_name}/rows", response_model=TableOperationResponse)
async def update_table_row(
    table_name: str,
    primary_key: Dict[str, Any],
    row_data: Dict[str, Any],
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """Update a row in the table"""
    result = db_manager.update_row(table_name, primary_key, row_data)
    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result['message']
        )
    return result

@router.delete("/tables/{table_name}/rows", response_model=TableOperationResponse)
async def delete_table_row(
    table_name: str,
    primary_key: Dict[str, Any],
    current_user: User = Depends(get_current_active_user)
) -> Dict[str, Any]:
    """Delete a row from the table"""
    result = db_manager.delete_row(table_name, primary_key)
    if not result['success']:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=result['message']
        )
    return result
