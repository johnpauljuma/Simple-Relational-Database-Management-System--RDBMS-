"""
Type definitions and data structures for MyRDBMS
"""

from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass
from enum import Enum

class DataType(Enum):
    """Supported data types"""
    INT = "INT"
    VARCHAR = "VARCHAR"
    TEXT = "TEXT"
    BOOLEAN = "BOOLEAN"
    DECIMAL = "DECIMAL"
    TIMESTAMP = "TIMESTAMP"
    DATE = "DATE"

class ConstraintType(Enum):
    """Column constraint types"""
    PRIMARY_KEY = "PRIMARY KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT NULL"
    FOREIGN_KEY = "FOREIGN KEY"
    CHECK = "CHECK"

class JoinType(Enum):
    """Join types"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"
    FULL = "FULL"

class IndexType(Enum):
    """Index types"""
    HASH = "HASH"
    BTREE = "BTREE"  # For future implementation

@dataclass
class ColumnDefinition:
    """Column definition for table schema"""
    name: str
    data_type: DataType
    constraints: List[ConstraintType]
    max_length: Optional[int] = None
    default_value: Any = None
    
    def __str__(self) -> str:
        constraints_str = ' '.join([c.value for c in self.constraints])
        type_str = f"{self.data_type.value}({self.max_length})" if self.max_length else self.data_type.value
        return f"{self.name} {type_str} {constraints_str}".strip()

@dataclass
class TableSchema:
    """Complete table schema"""
    name: str
    columns: List[ColumnDefinition]
    primary_key: Optional[str] = None
    indexes: Dict[str, IndexType] = None
    
    def __post_init__(self):
        if self.indexes is None:
            self.indexes = {}
        # Find primary key
        for col in self.columns:
            if ConstraintType.PRIMARY_KEY in col.constraints:
                self.primary_key = col.name
                break

@dataclass
class QueryResult:
    """Standardized query result"""
    success: bool
    data: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    message: Optional[str] = None
    row_count: int = 0
    execution_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to API-friendly dictionary"""
        return {
            'success': self.success,
            'data': self.data or [],
            'columns': self.columns or [],
            'message': self.message or '',
            'row_count': self.row_count,
            'execution_time': self.execution_time
        }

@dataclass
class IndexInfo:
    """Index information"""
    name: str
    table_name: str
    column_name: str
    index_type: IndexType
    is_unique: bool = False