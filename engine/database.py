"""
Main Database class - Facade for MyRDBMS
"""

import time
from typing import Dict, List, Any, Optional
from engine.storage import Storage
from engine.parser import SQLParser
from engine.query_executor import QueryExecutor
from engine.index_manager import IndexManager
from engine.types import QueryResult
from engine.errors import MyRDBMSError, ParseError, ExecutionError

class Database:
    """
    Main database interface with separated concerns.
    
    Architecture:
        Client → Database.execute() → Parser → QueryExecutor → Storage
                     ↑                                      ↓
                    Results ←-----------------------------
    """
    
    def __init__(self, name: str):
        self.name = name
        self.storage = Storage()
        self.parser = SQLParser()
        self.index_manager = IndexManager(self.storage)
        
        # Ensure database exists
        if not self.storage.database_exists(name):
            self.storage.create_database(name)
    
    # In engine/database.py, in the execute method:

    def execute(self, query: str) -> Dict[str, Any]:
        """Execute SQL query through separated pipeline."""
        start_time = time.time()
        
        try:
            # 1. Parse
            parsed_query = self.parser.parse(query)
            
            # 2. Execute - QueryExecutor only takes storage and db_name
            executor = QueryExecutor(self.storage, self.name)
            result = executor.execute(parsed_query)
            
            # 3. Format results
            execution_time = time.time() - start_time
            
            query_result = QueryResult(
                success='error' not in result,
                data=result.get('rows', []),
                columns=result.get('columns', []),
                message=result.get('message', 'Query executed successfully'),
                row_count=result.get('count', 0),
                execution_time=execution_time
            )
            
            return query_result.to_dict()
            
        except ParseError as e:
            return QueryResult(
                success=False,
                message=f"Parse error: {str(e)}",
                execution_time=time.time() - start_time
            ).to_dict()
            
        except Exception as e:
            return QueryResult(
                success=False,
                message=f"Unexpected error: {str(e)}",
                execution_time=time.time() - start_time
            ).to_dict()
    
    def explain(self, query: str) -> Dict[str, Any]:
        """Show execution plan for query"""
        try:
            parsed = self.parser.parse(query)
            
            plan = {
                'query_type': type(parsed).__name__,
                'components': []
            }
            
            # Analyze parsed query
            if hasattr(parsed, 'table_name'):
                plan['components'].append({
                    'operation': 'TABLE_ACCESS',
                    'table': parsed.table_name
                })
            
            if hasattr(parsed, 'where_clause') and parsed.where_clause:
                plan['components'].append({
                    'operation': 'FILTER',
                    'condition': parsed.where_clause
                })
            
            if hasattr(parsed, 'join_clause') and parsed.join_clause:
                plan['components'].append({
                    'operation': 'JOIN',
                    'type': parsed.join_clause.get('type', 'INNER'),
                    'table': parsed.join_clause['table']
                })
            
            return {'plan': plan}
            
        except Exception as e:
            return {'error': str(e)}
    
    def create_index(self, table_name: str, column: str, index_type: str = "HASH") -> bool:
        """Create index on table column"""
        return self.index_manager.create_index(self.name, table_name, column, index_type)
    
    def list_indexes(self, table_name: str) -> List[str]:
        """List indexes for table"""
        return self.index_manager.list_indexes(self.name, table_name)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        meta = self.storage._load_metadata(self.name)
        
        stats = {
            'name': self.name,
            'tables': [],
            'total_rows': 0
        }
        
        for table_name in meta.get('tables', []):
            rows = self.storage.get_all_rows(self.name, table_name)
            schema = self.storage.load_table_schema(self.name, table_name)
            
            stats['tables'].append({
                'name': table_name,
                'row_count': len(rows),
                'columns': len(schema.get('columns', [])) if schema else 0
            })
            stats['total_rows'] += len(rows)
        
        return stats
    
    def backup(self, backup_path: Optional[str] = None) -> bool:
        """Backup database (conceptual)"""
        # In production, would copy all files
        print(f"Backup concept: Would copy {self.name} database files")
        return True
    
    def restore(self, backup_path: str) -> bool:
        """Restore from backup (conceptual)"""
        print(f"Restore concept: Would restore from {backup_path}")
        return True