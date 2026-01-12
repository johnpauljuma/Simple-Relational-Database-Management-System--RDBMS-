import os
import json
import pickle
from typing import Dict, List, Any

class Storage:
    def __init__(self, data_dir='./data'):
        self.data_dir = data_dir
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
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
    
    def _get_db_path(self, db_name: str) -> str:
        return os.path.join(self.data_dir, db_name)
    
    def _save_metadata(self, db_name: str, metadata: Dict):
        """Save database metadata"""
        meta_path = os.path.join(self._get_db_path(db_name), 'meta.json')
        with open(meta_path, 'w') as f:
            json.dump(metadata, f)
    
    def _load_metadata(self, db_name: str) -> Dict:
        """Load database metadata"""
        meta_path = os.path.join(self._get_db_path(db_name), 'meta.json')
        if os.path.exists(meta_path):
            with open(meta_path, 'r') as f:
                return json.load(f)
        return {'tables': []}
    
    def save_table_schema(self, db_name: str, table_name: str, schema: Dict):
        """Save table schema (columns, types, constraints)"""
        table_dir = os.path.join(self._get_db_path(db_name), table_name)
        if not os.path.exists(table_dir):
            os.makedirs(table_dir)
        
        schema_path = os.path.join(table_dir, 'schema.json')
        with open(schema_path, 'w') as f:
            json.dump(schema, f)
        
        # Update metadata
        meta = self._load_metadata(db_name)
        if table_name not in meta['tables']:
            meta['tables'].append(table_name)
            self._save_metadata(db_name, meta)
    
    def load_table_schema(self, db_name: str, table_name: str) -> Dict:
        """Load table schema"""
        schema_path = os.path.join(self._get_db_path(db_name), table_name, 'schema.json')
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                return json.load(f)
        return {}
    
    def insert_row(self, db_name: str, table_name: str, row: Dict) -> bool:
        """Insert a row into table data file"""
        data_file = os.path.join(self._get_db_path(db_name), table_name, 'data.pkl')
        
        # Load existing data
        data = []
        if os.path.exists(data_file):
            with open(data_file, 'rb') as f:
                data = pickle.load(f)
        
        # Append new row
        data.append(row)
        
        # Save back
        with open(data_file, 'wb') as f:
            pickle.dump(data, f)
        
        # Update index file for primary key
        self._update_index(db_name, table_name, row)
        
        return True
    
    def _update_index(self, db_name: str, table_name: str, row: Dict):
        """Simple index for primary key lookups"""
        schema = self.load_table_schema(db_name, table_name)
        
        # Find primary key column
        pk_column = None
        for col in schema.get('columns', []):
            if 'PRIMARY KEY' in col.get('constraints', []):
                pk_column = col['name']
                break
        
        if pk_column and pk_column in row:
            index_file = os.path.join(self._get_db_path(db_name), table_name, 'index.pkl')
            index = {}
            if os.path.exists(index_file):
                with open(index_file, 'rb') as f:
                    index = pickle.load(f)
            
            # Simple hash index: pk_value -> row_index
            # In real implementation, you'd store position in file
            index[row[pk_column]] = len(self.get_all_rows(db_name, table_name)) - 1
            
            with open(index_file, 'wb') as f:
                pickle.dump(index, f)
    
    def get_all_rows(self, db_name: str, table_name: str) -> List[Dict]:
        """Get all rows from a table"""
        data_file = os.path.join(self._get_db_path(db_name), table_name, 'data.pkl')
        if os.path.exists(data_file):
            with open(data_file, 'rb') as f:
                return pickle.load(f)
        return []
    
    def delete_table(self, db_name: str, table_name: str) -> bool:
        """Delete a table"""
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