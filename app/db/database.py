from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, MetaData, Table, select, insert, update, delete, inspect
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.types import LargeBinary
import base64

class DatabaseManager:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, connect_args={"check_same_thread": False})
        self.metadata = MetaData()
        self.metadata.reflect(bind=self.engine)
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in the database"""
        inspector = inspect(self.engine)
        return inspector.get_table_names()
    
    def get_table_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """Get column information for a specific table"""
        if table_name not in self.metadata.tables:
            self.metadata.reflect(only=[table_name], bind=self.engine)
        
        table = self.metadata.tables[table_name]
        columns = []
        
        for column in table.columns:
            columns.append({
                'name': column.name,
                'type': str(column.type),
                'nullable': column.nullable,
                'primary_key': column.primary_key,
                'is_binary': isinstance(column.type, LargeBinary)
            })
        
        return columns
    
    def get_table_data(
        self, 
        table_name: str, 
        skip: int = 0, 
        limit: int = 100,
        filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Get paginated data from a table with optional filtering"""
        if table_name not in self.metadata.tables:
            self.metadata.reflect(only=[table_name], bind=self.engine)
        
        table = self.metadata.tables[table_name]
        
        # Build query
        query = select(table)
        
        # Apply filters if provided
        if filters:
            for column, value in filters.items():
                if column in table.columns:
                    query = query.where(table.columns[column] == value)
        
        # Get total count
        with self.engine.connect() as conn:
            total = conn.execute(select([table]).select_from(query.subquery()).with_only_columns([table.count()])).scalar()
        
        # Apply pagination
        query = query.offset(skip).limit(limit)
        
        # Execute query
        with self.engine.connect() as conn:
            result = conn.execute(query)
            columns = result.keys()
            rows = result.fetchall()
        
        # Convert rows to dictionaries and handle binary data
        processed_rows = []
        for row in rows:
            row_dict = {}
            for i, value in enumerate(row):
                col_name = columns[i]
                col_type = table.columns[col_name].type
                
                if isinstance(col_type, LargeBinary) and value is not None:
                    # Convert binary data to base64 for JSON serialization
                    row_dict[col_name] = {
                        'type': 'binary',
                        'data': base64.b64encode(value).decode('utf-8'),
                        'size': len(value)
                    }
                else:
                    row_dict[col_name] = value
            processed_rows.append(row_dict)
        
        return {
            'columns': [{'name': col, 'type': str(table.columns[col].type)} for col in columns],
            'rows': processed_rows,
            'count': total
        }
    
    def insert_row(self, table_name: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Insert a new row into the specified table"""
        if table_name not in self.metadata.tables:
            self.metadata.reflect(only=[table_name], bind=self.engine)
        
        table = self.metadata.tables[table_name]
        
        # Process binary data
        processed_data = {}
        for col_name, value in data.items():
            if col_name in table.columns and isinstance(table.columns[col_name].type, LargeBinary) and value:
                if isinstance(value, dict) and value.get('type') == 'binary' and 'data' in value:
                    # Decode base64 string back to binary
                    processed_data[col_name] = base64.b64decode(value['data'])
            else:
                processed_data[col_name] = value
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(insert(table).values(**processed_data))
                # Get the inserted row
                if result.inserted_primary_key:
                    pk_columns = [col.name for col in table.primary_key]
                    pk_values = result.inserted_primary_key
                    if len(pk_columns) == 1:
                        pk_condition = {pk_columns[0]: pk_values[0]}
                    else:
                        pk_condition = dict(zip(pk_columns, pk_values))
                    
                    # Fetch the inserted row
                    query = select(table).filter_by(**pk_condition)
                    inserted_row = conn.execute(query).fetchone()
                    
                    # Convert to dict
                    if inserted_row:
                        return {
                            'success': True,
                            'message': 'Row inserted successfully',
                            'data': dict(inserted_row._mapping)
                        }
            
            return {'success': True, 'message': 'Row inserted successfully'}
        except SQLAlchemyError as e:
            return {'success': False, 'message': f'Error inserting row: {str(e)}'}
    
    def update_row(
        self, 
        table_name: str, 
        primary_key: Dict[str, Any], 
        data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Update a row in the specified table"""
        if table_name not in self.metadata.tables:
            self.metadata.reflect(only=[table_name], bind=self.engine)
        
        table = self.metadata.tables[table_name]
        
        # Process binary data
        processed_data = {}
        for col_name, value in data.items():
            if col_name in table.columns and isinstance(table.columns[col_name].type, LargeBinary) and value:
                if isinstance(value, dict) and value.get('type') == 'binary' and 'data' in value:
                    # Decode base64 string back to binary
                    processed_data[col_name] = base64.b64decode(value['data'])
            else:
                processed_data[col_name] = value
        
        try:
            with self.engine.begin() as conn:
                # Build where clause from primary key
                conditions = []
                for col_name, value in primary_key.items():
                    if col_name in table.columns:
                        conditions.append(table.columns[col_name] == value)
                
                if not conditions:
                    return {'success': False, 'message': 'No valid primary key provided'}
                
                # Execute update
                stmt = update(table).where(*conditions).values(**processed_data)
                result = conn.execute(stmt)
                
                if result.rowcount == 0:
                    return {'success': False, 'message': 'No rows were updated'}
                
                return {'success': True, 'message': 'Row updated successfully'}
                
        except SQLAlchemyError as e:
            return {'success': False, 'message': f'Error updating row: {str(e)}'}
    
    def delete_row(self, table_name: str, primary_key: Dict[str, Any]) -> Dict[str, Any]:
        """Delete a row from the specified table"""
        if table_name not in self.metadata.tables:
            self.metadata.reflect(only=[table_name], bind=self.engine)
        
        table = self.metadata.tables[table_name]
        
        try:
            with self.engine.begin() as conn:
                # Build where clause from primary key
                conditions = []
                for col_name, value in primary_key.items():
                    if col_name in table.columns:
                        conditions.append(table.columns[col_name] == value)
                
                if not conditions:
                    return {'success': False, 'message': 'No valid primary key provided'}
                
                # Execute delete
                stmt = delete(table).where(*conditions)
                result = conn.execute(stmt)
                
                if result.rowcount == 0:
                    return {'success': False, 'message': 'No rows were deleted'}
                
                return {'success': True, 'message': 'Row deleted successfully'}
                
        except SQLAlchemyError as e:
            return {'success': False, 'message': f'Error deleting row: {str(e)}'}
