"""
Storage engine for MyRDBMS
Handles file I/O and persistence
"""

import os
import json
import pickle
from typing import Dict, List, Any, Optional
from engine.types import TableSchema, ColumnDefinition, ConstraintType, DataType
from engine.errors import StorageError, TableNotFoundError

class Storage:
    """File-based storage engine"""
    
    def __init__(self, data_dir='./data'):
        self.data_dir = data_dir
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
    # Database operations
    def create_database(self, db_name: str) -> bool:
        """Create a new database directory"""
        db_path = os.path.join(self.data_dir, db_name)
        if not os.path.exists(db_path):
            os.makedirs(db_path)
            self._save_metadata(db_name, {'tables': []})
            return True
        return False
    
    def list_databases(self) -> List[str]:
        """List all databases"""
        if os.path.exists(self.data_dir):
            return [d for d in os.listdir(self.data_dir) 
                   if os.path.isdir(os.path.join(self.data_dir, d))]
        return []
    
    def database_exists(self, db_name: str) -> bool:
        """Check if database exists"""
        return os.path.exists(os.path.join(self.data_dir, db_name))
    
    # Table operations
    def save_table_schema(self, db_name: str, table_name: str, schema: Dict):
        """Save table schema to disk"""
        table_dir = os.path.join(self._get_db_path(db_name), table_name)
        if not os.path.exists(table_dir):
            os.makedirs(table_dir)
        
        schema_path = os.path.join(table_dir, 'schema.json')
        with open(schema_path, 'w') as f:
            json.dump(self._serialize_schema(schema), f, indent=2)
        
        # Update metadata
        meta = self._load_metadata(db_name)
        if table_name not in meta['tables']:
            meta['tables'].append(table_name)
            self._save_metadata(db_name, meta)
    
    def load_table_schema(self, db_name: str, table_name: str) -> Optional[Dict]:
        """Load table schema from disk"""
        schema_path = os.path.join(self._get_db_path(db_name), table_name, 'schema.json')
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                return self._deserialize_schema(json.load(f))
        return None
    
    def _serialize_schema(self, schema: Dict) -> Dict:
        """Serialize schema for JSON storage"""
        if 'columns' in schema:
            schema['columns'] = [
                {
                    'name': col['name'],
                    'data_type': col['type'],
                    'constraints': col.get('constraints', []),
                    'max_length': col.get('max_length')
                }
                for col in schema['columns']
            ]
        return schema
    
    def _deserialize_schema(self, schema_dict: Dict) -> Dict:
        """Deserialize schema from JSON"""
        return schema_dict  # Add type conversion if needed
    
    # Data operations
    def insert_row(self, db_name: str, table_name: str, row: Dict) -> bool:
        """Insert a row into table"""
        data_file = os.path.join(self._get_db_path(db_name), table_name, 'data.pkl')
        
        # Load existing data
        data = []
        if os.path.exists(data_file):
            try:
                with open(data_file, 'rb') as f:
                    data = pickle.load(f)
            except:
                data = []
        
        # Append new row
        data.append(row)
        
        # Save back
        with open(data_file, 'wb') as f:
            pickle.dump(data, f)
        
        return True
    
    def get_all_rows(self, db_name: str, table_name: str) -> List[Dict]:
        """Get all rows from a table"""
        data_file = os.path.join(self._get_db_path(db_name), table_name, 'data.pkl')
        if os.path.exists(data_file):
            try:
                with open(data_file, 'rb') as f:
                    return pickle.load(f)
            except:
                return []
        return []
    
    def update_rows(self, db_name: str, table_name: str, 
                   updates: List[Dict]) -> bool:
        """Update rows in table"""
        data_file = os.path.join(self._get_db_path(db_name), table_name, 'data.pkl')
        
        if os.path.exists(data_file):
            with open(data_file, 'rb') as f:
                data = pickle.load(f)
            
            # Apply updates
            for update in updates:
                idx = update.get('index')
                new_row = update.get('row')
                if idx is not None and idx < len(data) and new_row:
                    data[idx] = new_row
            
            # Save back
            with open(data_file, 'wb') as f:
                pickle.dump(data, f)
            
            return True
        
        return False
    
    def delete_table(self, db_name: str, table_name: str) -> bool:
        """Delete a table and all its data"""
        table_dir = os.path.join(self._get_db_path(db_name), table_name)
        if os.path.exists(table_dir):
            import shutil
            shutil.rmtree(table_dir)
            
            # Update metadata
            meta = self._load_metadata(db_name)
            if table_name in meta['tables']:
                meta['tables'].remove(table_name)
                self._save_metadata(db_name, meta)
            return True
        return False
    
    # Helper methods
    def _get_db_path(self, db_name: str) -> str:
        return os.path.join(self.data_dir, db_name)
    
    def _save_metadata(self, db_name: str, metadata: Dict):
        """Save database metadata"""
        meta_path = os.path.join(self._get_db_path(db_name), 'meta.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def _load_metadata(self, db_name: str) -> Dict:
        """Load database metadata"""
        meta_path = os.path.join(self._get_db_path(db_name), 'meta.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                return json.load(f)
        return {'tables': []}
    
    def table_exists(self, db_name: str, table_name: str) -> bool:
        """Check if table exists"""
        table_dir = os.path.join(self._get_db_path(db_name), table_name)
        return os.path.exists(table_dir)