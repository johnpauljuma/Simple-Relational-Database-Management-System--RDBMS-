from typing import Dict, List, Any

class Table:
    def __init__(self, name: str, columns: List[Dict]):
        self.name = name
        self.columns = columns
        self.data = []
        self.indexes = {}
        
    def validate_row(self, row: Dict) -> bool:
        """Validate row against schema"""
        # Check all required columns are present
        for col in self.columns:
            if col.get('not_null', False) and col['name'] not in row:
                return False
            
            # Type checking (simplified)
            if col['name'] in row:
                value = row[col['name']]
                col_type = col['type'].upper()
                
                if col_type == 'INT' and not isinstance(value, int):
                    return False
                elif col_type == 'VARCHAR' and not isinstance(value, str):
                    return False
                elif col_type == 'BOOLEAN' and not isinstance(value, bool):
                    return False
        
        # Check primary key uniqueness (simplified)
        pk_columns = [c['name'] for c in self.columns if 'PRIMARY KEY' in c.get('constraints', [])]
        for pk in pk_columns:
            if pk in row:
                # Check if value already exists
                for existing_row in self.data:
                    if existing_row.get(pk) == row[pk]:
                        return False
        
        return True
    
    def insert(self, row: Dict) -> bool:
        """Insert a row into the table"""
        if self.validate_row(row):
            self.data.append(row)
            # Update indexes
            self._update_indexes(row)
            return True
        return False
    
    def _update_indexes(self, row: Dict):
        """Update all indexes for the table"""
        for index_col in self.indexes:
            if index_col in row:
                value = row[index_col]
                if value not in self.indexes[index_col]:
                    self.indexes[index_col][value] = []
                self.indexes[index_col][value].append(len(self.data) - 1)
    
    def select(self, where_clause=None) -> List[Dict]:
        """Select rows with optional filtering"""
        if where_clause is None:
            return self.data.copy()
        
        # Simple WHERE clause parser (just column=value for now)
        results = []
        for row in self.data:
            if self._evaluate_where(row, where_clause):
                results.append(row)
        return results
    
    def _evaluate_where(self, row: Dict, where_clause: str) -> bool:
        """Evaluate a simple WHERE clause"""
        try:
            # Simple: "column = value"
            if '=' in where_clause:
                col, value = where_clause.split('=', 1)
                col = col.strip()
                value = value.strip().strip("'")
                
                if col in row:
                    return str(row[col]) == value
        except:
            pass
        return False
    
    def create_index(self, column: str):
        """Create an index on a column"""
        if column not in self.indexes:
            self.indexes[column] = {}
            # Build index from existing data
            for i, row in enumerate(self.data):
                if column in row:
                    value = row[column]
                    if value not in self.indexes[column]:
                        self.indexes[column][value] = []
                    self.indexes[column][value].append(i)