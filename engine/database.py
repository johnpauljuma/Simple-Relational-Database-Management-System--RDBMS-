from typing import Dict, List, Any
from engine.storage import Storage
from engine.table import Table
import json

class Database:
    def __init__(self, name: str):
        self.name = name
        self.storage = Storage()
        self.tables = {}
        self._load_tables()
    
    def _load_tables(self):
        """Load all tables from storage"""
        # In a real implementation, you'd load from disk
        pass
    
    def create_table(self, table_name: str, columns: List[Dict]) -> bool:
        """Create a new table"""
        # Store schema
        schema = {
            'name': table_name,
            'columns': columns
        }
        self.storage.save_table_schema(self.name, table_name, schema)
        
        # Create in-memory table
        self.tables[table_name] = Table(table_name, columns)
        return True
    
    def execute(self, query: str) -> Dict:
        """Execute a SQL-like query"""
        query = query.strip().upper()
        
        if query.startswith('CREATE TABLE'):
            return self._execute_create_table(query)
        elif query.startswith('INSERT INTO'):
            return self._execute_insert(query)
        elif query.startswith('SELECT'):
            return self._execute_select(query)
        elif query.startswith('DROP TABLE'):
            return self._execute_drop_table(query)
        else:
            return {'error': f'Unknown query: {query}'}
    
    def _execute_create_table(self, query: str) -> Dict:
        """Parse and execute CREATE TABLE"""
        try:
            # Simple parser for: CREATE TABLE table_name (col1 TYPE, col2 TYPE)
            parts = query.split('(', 1)
            table_part = parts[0].replace('CREATE TABLE', '').strip()
            table_name = table_part.split()[0].strip()
            
            cols_part = parts[1].rsplit(')', 1)[0]
            columns = []
            
            for col_def in cols_part.split(','):
                col_def = col_def.strip()
                if col_def:
                    col_parts = col_def.split()
                    col_name = col_parts[0]
                    col_type = col_parts[1] if len(col_parts) > 1 else 'TEXT'
                    
                    constraints = []
                    if len(col_parts) > 2:
                        constraints = col_parts[2:]
                    
                    columns.append({
                        'name': col_name,
                        'type': col_type,
                        'constraints': constraints
                    })
            
            self.create_table(table_name, columns)
            return {'message': f'Table {table_name} created successfully'}
        except Exception as e:
            return {'error': f'Error creating table: {str(e)}'}
    
    def _execute_insert(self, query: str) -> Dict:
        """Parse and execute INSERT"""
        try:
            # Parse: INSERT INTO table_name VALUES (val1, val2, ...)
            query = query.replace('INSERT INTO', '').strip()
            table_name = query.split()[0].strip()
            
            # Find values part
            start = query.find('VALUES')
            if start == -1:
                start = query.find('VALUES')
            
            values_str = query[start + 6:].strip()
            values_str = values_str.strip('()')
            
            # Parse values
            values = []
            current = ''
            in_quotes = False
            
            for char in values_str:
                if char == "'" and not in_quotes:
                    in_quotes = True
                    current += char
                elif char == "'" and in_quotes:
                    in_quotes = False
                    current += char
                elif char == ',' and not in_quotes:
                    values.append(current.strip())
                    current = ''
                else:
                    current += char
            
            if current:
                values.append(current.strip())
            
            # Convert values to appropriate types
            typed_values = []
            for v in values:
                if v.startswith("'") and v.endswith("'"):
                    typed_values.append(v[1:-1])  # Remove quotes
                elif v.lower() == 'true':
                    typed_values.append(True)
                elif v.lower() == 'false':
                    typed_values.append(False)
                elif '.' in v:
                    typed_values.append(float(v))
                else:
                    typed_values.append(int(v) if v.isdigit() else v)
            
            # Get column names from schema
            schema = self.storage.load_table_schema(self.name, table_name)
            column_names = [col['name'] for col in schema.get('columns', [])]
            
            # Create row dict
            if len(typed_values) != len(column_names):
                return {'error': f'Expected {len(column_names)} values, got {len(typed_values)}'}
            
            row = {}
            for i, col_name in enumerate(column_names):
                row[col_name] = typed_values[i]
            
            # Insert into storage
            self.storage.insert_row(self.name, table_name, row)
            
            return {'message': '1 row inserted'}
        except Exception as e:
            return {'error': f'Error inserting: {str(e)}'}
    
    def _execute_select(self, query: str) -> Dict:
        """Parse and execute SELECT"""
        try:
            # Simple: SELECT * FROM table_name WHERE column=value
            query = query.replace('SELECT', '').strip()
            
            from_idx = query.find('FROM')
            if from_idx == -1:
                return {'error': 'Missing FROM clause'}
            
            columns_part = query[:from_idx].strip()
            rest = query[from_idx + 4:].strip()
            
            # Get table name
            where_idx = rest.find('WHERE')
            if where_idx != -1:
                table_name = rest[:where_idx].strip()
                where_clause = rest[where_idx + 5:].strip()
            else:
                table_name = rest.strip()
                where_clause = None
            
            # Get all rows
            rows = self.storage.get_all_rows(self.name, table_name)
            
            # Apply WHERE clause
            if where_clause:
                filtered_rows = []
                for row in rows:
                    if '=' in where_clause:
                        col, value = where_clause.split('=', 1)
                        col = col.strip()
                        value = value.strip().strip("'")
                        if str(row.get(col, '')) == value:
                            filtered_rows.append(row)
                rows = filtered_rows
            
            return {
                'columns': list(rows[0].keys()) if rows else [],
                'rows': rows,
                'count': len(rows)
            }
        except Exception as e:
            return {'error': f'Error selecting: {str(e)}'}
    
    def _execute_drop_table(self, query: str) -> Dict:
        """Execute DROP TABLE"""
        try:
            table_name = query.replace('DROP TABLE', '').strip()
            success = self.storage.delete_table(self.name, table_name)
            if success:
                return {'message': f'Table {table_name} dropped'}
            else:
                return {'error': f'Table {table_name} not found'}
        except Exception as e:
            return {'error': str(e)}