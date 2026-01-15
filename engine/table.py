"""
Table class with schema validation and row operations
"""

from typing import Dict, List, Any, Optional
from engine.types import TableSchema, ColumnDefinition, ConstraintType, DataType
from engine.errors import ConstraintError, SchemaError

class Table:
    """In-memory table representation with validation"""
    
    def __init__(self, name: str, schema: TableSchema):
        self.name = name
        self.schema = schema
        self.rows: List[Dict[str, Any]] = []
        self.indexes = {}
        
        # Build primary key index
        if schema.primary_key:
            self.indexes[schema.primary_key] = {}
    
    def validate_row(self, row: Dict[str, Any]) -> bool:
        """Validate row against schema"""
        # Check all required columns are present
        for col in self.schema.columns:
            if ConstraintType.NOT_NULL in col.constraints and col.name not in row:
                raise ConstraintError(f"Column '{col.name}' cannot be NULL")
            
            if col.name in row:
                value = row[col.name]
                
                # Type validation
                if not self._validate_type(value, col.data_type):
                    raise SchemaError(f"Invalid type for column '{col.name}': expected {col.data_type.value}")
                
                # Length validation for VARCHAR
                if col.data_type == DataType.VARCHAR and col.max_length:
                    if len(str(value)) > col.max_length:
                        raise ConstraintError(f"Value too long for column '{col.name}': max {col.max_length}")
        
        # Check primary key uniqueness
        if self.schema.primary_key and self.schema.primary_key in row:
            pk_value = row[self.schema.primary_key]
            if self._pk_exists(pk_value):
                raise ConstraintError(f"Duplicate primary key value: {pk_value}")
        
        return True
    
    def _validate_type(self, value: Any, data_type: DataType) -> bool:
        """Validate value against data type"""
        if value is None:
            return True
        
        try:
            if data_type == DataType.INT:
                return isinstance(value, int)
            elif data_type == DataType.VARCHAR:
                return isinstance(value, str)
            elif data_type == DataType.TEXT:
                return isinstance(value, str)
            elif data_type == DataType.BOOLEAN:
                return isinstance(value, bool)
            elif data_type == DataType.DECIMAL:
                return isinstance(value, (int, float))
            elif data_type == DataType.TIMESTAMP:
                return isinstance(value, str)  # Simplified
            elif data_type == DataType.DATE:
                return isinstance(value, str)  # Simplified
        except:
            return False
        
        return True
    
    def _pk_exists(self, pk_value: Any) -> bool:
        """Check if primary key value already exists"""
        for row in self.rows:
            if row.get(self.schema.primary_key) == pk_value:
                return True
        return False
    
    def insert(self, row: Dict[str, Any]) -> int:
        """Insert a validated row"""
        self.validate_row(row)
        
        # Add row
        self.rows.append(row.copy())
        row_index = len(self.rows) - 1
        
        # Update indexes
        self._update_indexes(row, row_index)
        
        return row_index
    
    def _update_indexes(self, row: Dict[str, Any], row_index: int):
        """Update all indexes for the table"""
        for col_name, index in self.indexes.items():
            if col_name in row:
                value = row[col_name]
                if value not in index:
                    index[value] = []
                index[value].append(row_index)
    
    def select(self, where_clause: Optional[str] = None) -> List[Dict[str, Any]]:
        """Select rows with optional filtering"""
        if where_clause is None:
            return self.rows.copy()
        
        # Simple WHERE parsing
        if '=' in where_clause:
            col, value = where_clause.split('=', 1)
            col = col.strip()
            value = value.strip().strip("'")
            
            # Use index if available
            if col in self.indexes:
                row_indices = self.indexes[col].get(value, [])
                return [self.rows[i] for i in row_indices]
            
            # Otherwise filter manually
            return [row for row in self.rows if str(row.get(col, '')) == value]
        
        return []
    
    def update(self, set_clause: Dict[str, Any], where_clause: Optional[str] = None) -> int:
        """Update rows matching WHERE clause"""
        updated_count = 0
        
        for i, row in enumerate(self.rows):
            # Check WHERE condition
            if where_clause and '=' in where_clause:
                col, value = where_clause.split('=', 1)
                col = col.strip()
                value = value.strip().strip("'")
                if str(row.get(col, '')) != value:
                    continue
            
            # Update row
            for col, new_value in set_clause.items():
                row[col] = new_value
            
            updated_count += 1
        
        return updated_count
    
    def delete(self, where_clause: Optional[str] = None) -> int:
        """Delete rows matching WHERE clause"""
        if where_clause is None:
            deleted_count = len(self.rows)
            self.rows.clear()
            self.indexes.clear()
            return deleted_count
        
        # Simple WHERE parsing for equality
        if '=' in where_clause:
            col, value = where_clause.split('=', 1)
            col = col.strip()
            value = value.strip().strip("'")
            
            # Remove matching rows
            new_rows = []
            for row in self.rows:
                if str(row.get(col, '')) != value:
                    new_rows.append(row)
            
            deleted_count = len(self.rows) - len(new_rows)
            self.rows = new_rows
            
            # Rebuild indexes
            self._rebuild_indexes()
            
            return deleted_count
        
        return 0
    
    def _rebuild_indexes(self):
        """Rebuild all indexes from current rows"""
        self.indexes.clear()
        if self.schema.primary_key:
            self.indexes[self.schema.primary_key] = {}
        
        for i, row in enumerate(self.rows):
            self._update_indexes(row, i)
    
    def create_index(self, column: str, index_type: str = "HASH"):
        """Create an index on a column"""
        if column not in self.indexes:
            self.indexes[column] = {}
            
            # Build index from existing data
            for i, row in enumerate(self.rows):
                if column in row:
                    value = row[column]
                    if value not in self.indexes[column]:
                        self.indexes[column][value] = []
                    self.indexes[column][value].append(i)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get table statistics"""
        return {
            'name': self.name,
            'row_count': len(self.rows),
            'columns': [str(col) for col in self.schema.columns],
            'indexes': list(self.indexes.keys()),
            'primary_key': self.schema.primary_key
        }