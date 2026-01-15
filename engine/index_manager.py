"""
Index Manager for MyRDBMS
Handles creation and use of indexes
"""

import pickle
import os
from typing import Dict, List, Any

class IndexManager:
    """Manages database indexes for faster lookups"""
    
    def __init__(self, storage):
        self.storage = storage
    
    def create_index(self, db_name: str, table_name: str, column: str, index_type: str = "HASH") -> bool:
        """Create an index on a column"""
        # Get all rows
        rows = self.storage.get_all_rows(db_name, table_name)
        
        # Build index (hash map)
        index = {}
        for i, row in enumerate(rows):
            key = row.get(column)
            if key is not None:  # Skip NULL values
                if key not in index:
                    index[key] = []
                index[key].append(i)
        
        # Save index
        index_dir = os.path.join(
            self.storage._get_db_path(db_name), 
            table_name
        )
        os.makedirs(index_dir, exist_ok=True)
        
        index_file = os.path.join(index_dir, f'index_{column}.pkl')
        
        with open(index_file, 'wb') as f:
            pickle.dump(index, f)
        
        return True
    
    def get_by_index(self, db_name: str, table_name: str, 
                    column: str, value: Any) -> List[Dict]:
        """Get rows using index"""
        # Load index
        index_file = os.path.join(
            self.storage._get_db_path(db_name),
            table_name,
            f'index_{column}.pkl'
        )
        
        if not os.path.exists(index_file):
            return []  # No index
        
        with open(index_file, 'rb') as f:
            index = pickle.load(f)
        
        if value not in index:
            return []
        
        # Get all rows
        rows = self.storage.get_all_rows(db_name, table_name)
        
        # Return indexed rows
        return [rows[i] for i in index[value]]
    
    def drop_index(self, db_name: str, table_name: str, column: str) -> bool:
        """Remove an index"""
        index_file = os.path.join(
            self.storage._get_db_path(db_name),
            table_name,
            f'index_{column}.pkl'
        )
        
        if os.path.exists(index_file):
            os.remove(index_file)
            return True
        
        return False
    
    def list_indexes(self, db_name: str, table_name: str) -> List[str]:
        """List all indexes for a table"""
        table_dir = os.path.join(self.storage._get_db_path(db_name), table_name)
        if not os.path.exists(table_dir):
            return []
        
        indexes = []
        for filename in os.listdir(table_dir):
            if filename.startswith('index_') and filename.endswith('.pkl'):
                indexes.append(filename[6:-4])  # Remove 'index_' and '.pkl'
        
        return indexes