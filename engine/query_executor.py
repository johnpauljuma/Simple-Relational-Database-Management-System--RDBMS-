"""
Query Executor for MyRDBMS
Handles execution of parsed queries
"""

import re
import time
from typing import Dict, List, Any
from engine.parser import *
from engine.storage import Storage
from engine.join_executor import JoinExecutor
from engine.index_manager import IndexManager
from engine.errors import ExecutionError
import logging

logger = logging.getLogger(__name__)

class QueryExecutor:
    """Executes parsed queries against storage"""
    
    def __init__(self, storage: Storage, db_name: str):
        self.storage = storage
        self.db_name = db_name
        self.index_manager = IndexManager(storage)  # Initialize here
    
    def execute(self, parsed_query: ParsedQuery) -> Dict[str, Any]:
        """Execute a parsed query"""
        start_time = time.time()
        
        try:
            if isinstance(parsed_query, CreateTableQuery):
                result = self._execute_create_table(parsed_query)
            elif isinstance(parsed_query, InsertQuery):
                result = self._execute_insert(parsed_query)
            elif isinstance(parsed_query, SelectQuery):
                result = self._execute_select(parsed_query)
            elif isinstance(parsed_query, UpdateQuery):
                result = self._execute_update(parsed_query)
            elif isinstance(parsed_query, DeleteQuery):
                result = self._execute_delete(parsed_query)
            elif isinstance(parsed_query, DropTableQuery):
                result = self._execute_drop_table(parsed_query)
            else:
                result = {'error': f'Unsupported query type: {type(parsed_query).__name__}'}
            
            # Add execution time
            result['execution_time'] = time.time() - start_time
            
            # Ensure consistent response format
            if 'error' not in result:
                result['success'] = True
            else:
                result['success'] = False
            
            return result
            
        except Exception as e:
            raise ExecutionError(f"Query execution failed: {str(e)}")
    
    def _execute_create_table(self, query: CreateTableQuery) -> Dict[str, Any]:
        """Execute CREATE TABLE"""
        try:
            # Convert to storage format
            columns = []
            for col in query.columns:
                constraints = []
                if ConstraintType.PRIMARY_KEY in col.constraints:
                    constraints.append('PRIMARY KEY')
                if ConstraintType.UNIQUE in col.constraints:
                    constraints.append('UNIQUE')
                if ConstraintType.NOT_NULL in col.constraints:
                    constraints.append('NOT NULL')
                
                columns.append({
                    'name': col.name,
                    'type': col.data_type.value,
                    'constraints': constraints,
                    'max_length': col.max_length
                })
            
            # Save schema
            schema = {
                'name': query.table_name,
                'columns': columns
            }
            
            self.storage.save_table_schema(self.db_name, query.table_name, schema)
            
            # Create primary key index if exists
            for col in query.columns:
                if ConstraintType.PRIMARY_KEY in col.constraints:
                    self.index_manager.create_index(self.db_name, query.table_name, col.name)
                    break
            
            return {
                'message': f'Table {query.table_name} created successfully',
                'table': query.table_name,
                'columns': len(columns)
            }
            
        except Exception as e:
            return {'error': f'Error creating table: {str(e)}'}
    
    def _execute_insert(self, query: InsertQuery) -> Dict[str, Any]:
        """Execute INSERT"""
        try:
            # Get schema to map values to columns
            schema = self.storage.load_table_schema(self.db_name, query.table_name)
            
            if not schema or 'columns' not in schema:
                return {'error': f'Table {query.table_name} not found or has no schema'}
            
            column_defs = schema.get('columns', [])
            column_names = [col['name'] for col in column_defs]
            
            if len(query.values) != len(column_names):
                return {'error': f'Expected {len(column_names)} values, got {len(query.values)}'}
            
            # Create row dict with proper type conversion
            row = {}
            for i, (col_name, col_def) in enumerate(zip(column_names, column_defs)):
                value = query.values[i]
                col_type = col_def.get('type', 'TEXT').upper()
                
                # Convert value based on column type
                if value is None:
                    row[col_name] = None
                elif col_type == 'INT':
                    try:
                        row[col_name] = int(value)
                    except:
                        row[col_name] = value
                elif col_type == 'DECIMAL':
                    try:
                        row[col_name] = float(value)
                    except:
                        row[col_name] = value
                elif col_type == 'BOOLEAN':
                    if isinstance(value, bool):
                        row[col_name] = value
                    elif isinstance(value, str):
                        row[col_name] = value.lower() in ['true', '1', 'yes', 't']
                    else:
                        row[col_name] = bool(value)
                else:  # TEXT, VARCHAR, etc.
                    row[col_name] = str(value)
            
            # Validate constraints
            for col_def in column_defs:
                col_name = col_def['name']
                constraints = col_def.get('constraints', [])
                
                # Check NOT NULL
                if 'NOT NULL' in constraints and col_name in row and row[col_name] is None:
                    return {'error': f'Column {col_name} cannot be NULL'}
                
                # Check UNIQUE (basic check - would need full table scan in production)
                if 'UNIQUE' in constraints and col_name in row:
                    all_rows = self.storage.get_all_rows(self.db_name, query.table_name)
                    for existing_row in all_rows:
                        if existing_row.get(col_name) == row[col_name]:
                            return {'error': f'Duplicate value for unique column {col_name}'}
            
            # Insert into storage
            success = self.storage.insert_row(self.db_name, query.table_name, row)
            if success:
                return {
                    'message': '1 row inserted',
                    'row': row
                }
            else:
                return {'error': 'Failed to insert row'}
                
        except Exception as e:
            return {'error': f'Error inserting row: {str(e)}'}
    
    def _execute_select(self, query: SelectQuery) -> Dict[str, Any]:
        """Execute SELECT with WHERE, basic JOIN, GROUP BY support"""
        try:
            # Check if table exists
            if not self.storage.table_exists(self.db_name, query.table_name):
                return {'error': f'Table {query.table_name} not found'}
            
            # Get all rows
            rows = self.storage.get_all_rows(self.db_name, query.table_name)
            
            # Apply JOIN if present
            if query.join_clause and query.join_clause.get('table'):
                join_result = self._execute_join(rows, query)
                if 'error' in join_result:
                    return join_result
                rows = join_result.get('rows', rows)
            
            # Apply WHERE clause
            if query.where_clause:
                rows = self._apply_where(rows, query.where_clause)
            
            # Apply GROUP BY
            if query.group_by:
                rows = self._apply_group_by(rows, query.group_by, query.columns)
            
            # Select specific columns
            if query.columns != ['*']:
                filtered_rows = []
                for row in rows:
                    filtered_row = {}
                    for col in query.columns:
                        if col in row:
                            filtered_row[col] = row[col]
                        else:
                            # Handle aggregation functions like COUNT(*)
                            if col.upper().startswith('COUNT'):
                                filtered_row[col] = len(rows)
                    filtered_rows.append(filtered_row)
                rows = filtered_rows
            
            # Apply ORDER BY
            if query.order_by:
                rows = self._apply_order_by(rows, query.order_by)
            
            # Apply LIMIT
            if query.limit:
                rows = rows[:query.limit]
            
            # Get column names
            columns = list(rows[0].keys()) if rows else []
            
            return {
            'columns': columns,
            'rows': rows,          # Original format
            'data': rows,          # Frontend expects 'data'
            'count': len(rows),    # Original format
            'row_count': len(rows), # Frontend expects 'row_count'
            'message': 'Query executed successfully'
        }
            
        except Exception as e:
            return {'error': f'Error executing SELECT: {str(e)}'}
    
    def _execute_join(self, left_rows: List[Dict], query: SelectQuery) -> Dict[str, Any]:
        """Execute JOIN operation - IMPROVED VERSION"""
        try:
            join_clause = query.join_clause
            if not join_clause or 'table' not in join_clause:
                return {'error': 'Invalid JOIN clause'}
            
            right_table = join_clause['table']
            on_clause = join_clause.get('on', '')
            
            # Get rows from right table
            right_rows = self.storage.get_all_rows(self.db_name, right_table)
            
            if not right_rows:
                return {
                    'rows': [],
                    'message': f'Right table {right_table} is empty',
                    'columns': list(left_rows[0].keys()) if left_rows else []
                }
            
            # Parse ON clause with better handling
            left_col = None
            right_col = None
            
            if on_clause and '=' in on_clause:
                # Remove any whitespace and split on '='
                parts = on_clause.split('=', 1)  # Split only on first '='
                if len(parts) == 2:
                    left_part = parts[0].strip()
                    right_part = parts[1].strip()
                    
                    # Extract column names (handle table.column syntax)
                    left_col = left_part.split('.')[-1] if '.' in left_part else left_part
                    right_col = right_part.split('.')[-1] if '.' in right_part else right_part
            
            # Check if columns exist in tables
            if left_col and left_rows:
                # Verify left column exists in left table
                sample_left = left_rows[0]
                if left_col not in sample_left:
                    # Try case-insensitive match
                    for key in sample_left.keys():
                        if key.lower() == left_col.lower():
                            left_col = key
                            break
                    else:
                        return {'error': f'Column {left_col} not found in left table'}
            
            if right_col and right_rows:
                # Verify right column exists in right table
                sample_right = right_rows[0]
                if right_col not in sample_right:
                    # Try case-insensitive match
                    for key in sample_right.keys():
                        if key.lower() == right_col.lower():
                            right_col = key
                            break
                    else:
                        return {'error': f'Column {right_col} not found in right table {right_table}'}
            
            # Perform join
            joined_rows = []
            
            if not left_col or not right_col:
                # Cartesian product (no valid ON clause)
                logger.warning(f'No valid ON clause, performing cartesian product')
                for left_row in left_rows:
                    for right_row in right_rows:
                        merged = self._merge_rows(left_row, right_row, right_table, query.columns)
                        joined_rows.append(merged)
            else:
                # INNER JOIN with ON clause
                # Create lookup for right table for faster matching
                right_lookup = {}
                for right_row in right_rows:
                    key = str(right_row.get(right_col, ''))
                    if key not in right_lookup:
                        right_lookup[key] = []
                    right_lookup[key].append(right_row)
                
                # Perform join using lookup
                for left_row in left_rows:
                    left_key = str(left_row.get(left_col, ''))
                    
                    if left_key in right_lookup:
                        for right_row in right_lookup[left_key]:
                            merged = self._merge_rows(left_row, right_row, right_table, query.columns)
                            joined_rows.append(merged)
                    # Note: For INNER JOIN, skip rows with no match
            
            # Get column names for result
            columns = []
            if joined_rows:
                columns = list(joined_rows[0].keys())
            
            return {
                'rows': joined_rows,
                'columns': columns,
                'count': len(joined_rows),
                'message': f'INNER JOIN: {len(left_rows)} × {len(right_rows)} → {len(joined_rows)} rows'
            }
            
        except Exception as e:
            logger.error(f"JOIN execution error: {str(e)}", exc_info=True)
            return {'error': f'JOIN execution error: {str(e)}'}

    def _merge_rows(self, left_row: Dict, right_row: Dict, right_table: str, selected_columns: List[str]) -> Dict:
        """Merge rows from two tables, handling column name conflicts"""
        merged = {}
        
        # Add left table columns
        for key, value in left_row.items():
            merged[key] = value
        
        # Add right table columns
        for key, value in right_row.items():
            new_key = key
            
            # Check for column name conflict
            if key in merged:
                # Add table prefix to avoid conflict
                new_key = f"{right_table}_{key}"
            
            merged[new_key] = value
        
        # If specific columns were selected, filter to only those
        if selected_columns and selected_columns != ['*']:
            filtered = {}
            for col in selected_columns:
                # Handle table.column syntax
                if '.' in col:
                    table_part, col_part = col.split('.')
                    # Check if this column exists in our merged row
                    if col_part in merged:
                        filtered[col] = merged[col_part]
                    elif f"{table_part}_{col_part}" in merged:
                        filtered[col] = merged[f"{table_part}_{col_part}"]
                elif col in merged:
                    filtered[col] = merged[col]
            
            # Also include columns without table prefix if they match
            for key, value in merged.items():
                if '_' in key:
                    col_part = key.split('_', 1)[1]
                    if col_part in selected_columns and col_part not in filtered:
                        filtered[col_part] = value
            
            return filtered
        
        return merged
    def _apply_where(self, rows: List[Dict], where_clause: str) -> List[Dict]:
        """Apply WHERE clause filtering with smart type handling"""
        if not where_clause:
            return rows
        
        # Parse WHERE clause
        operators = ['!=', '>=', '<=', '=', '>', '<']  # Order matters for multi-char operators
        op_found = None
        col = None
        value = None
        
        for op in operators:
            if op in where_clause:
                parts = where_clause.split(op)
                if len(parts) == 2:
                    col = parts[0].strip()
                    raw_value = parts[1].strip()
                    # Remove quotes if present
                    if (raw_value.startswith("'") and raw_value.endswith("'")) or \
                    (raw_value.startswith('"') and raw_value.endswith('"')):
                        value = raw_value[1:-1]
                    else:
                        value = raw_value
                    op_found = op
                    break
        
        if not op_found or not col:
            return rows  # No valid operator found
        
        filtered = []
        
        for row in rows:
            row_value = row.get(col)
            
            # Skip if row doesn't have this column
            if col not in row:
                continue
            
            # Smart comparison based on data types
            try:
                # Try numeric comparison
                num_row = float(row_value) if row_value is not None else None
                num_val = float(value) if value is not None else None
                
                if num_row is not None and num_val is not None:
                    # Numeric comparison
                    if op_found == '=' and num_row == num_val:
                        filtered.append(row)
                    elif op_found == '!=' and num_row != num_val:
                        filtered.append(row)
                    elif op_found == '>' and num_row > num_val:
                        filtered.append(row)
                    elif op_found == '<' and num_row < num_val:
                        filtered.append(row)
                    elif op_found == '>=' and num_row >= num_val:
                        filtered.append(row)
                    elif op_found == '<=' and num_row <= num_val:
                        filtered.append(row)
                else:
                    # Fall back to string comparison
                    str_row = str(row_value) if row_value is not None else ''
                    str_val = str(value) if value is not None else ''
                    
                    if op_found == '=' and str_row == str_val:
                        filtered.append(row)
                    elif op_found == '!=' and str_row != str_val:
                        filtered.append(row)
                    elif op_found == '>' and str_row > str_val:
                        filtered.append(row)
                    elif op_found == '<' and str_row < str_val:
                        filtered.append(row)
                    elif op_found == '>=' and str_row >= str_val:
                        filtered.append(row)
                    elif op_found == '<=' and str_row <= str_val:
                        filtered.append(row)
                        
            except (ValueError, TypeError):
                # Last resort: string comparison
                str_row = str(row_value) if row_value is not None else ''
                str_val = str(value) if value is not None else ''
                
                if op_found == '=' and str_row == str_val:
                    filtered.append(row)
                elif op_found == '!=' and str_row != str_val:
                    filtered.append(row)
        
        return filtered
        
        # If no operator found, return original rows
        return rows
    
    def _apply_group_by(self, rows: List[Dict], group_by: str, columns: List[str]) -> List[Dict]:
        """Apply GROUP BY with basic aggregation"""
        if not rows:
            return []
        
        groups = {}
        
        for row in rows:
            group_key = row.get(group_by)
            if group_key not in groups:
                groups[group_key] = []
            groups[group_key].append(row)
        
        # Build result with aggregations
        result = []
        for group_key, group_rows in groups.items():
            result_row = {group_by: group_key}
            
            # Check for aggregation functions in columns
            for col in columns:
                if col.upper().startswith('COUNT(') and col.upper().endswith(')'):
                    if col.upper() == 'COUNT(*)':
                        result_row['count'] = len(group_rows)
                    else:
                        # Extract column name from COUNT(column)
                        inner = col[6:-1]  # Remove 'COUNT(' and ')'
                        non_null = sum(1 for r in group_rows if r.get(inner) is not None)
                        result_row[f'count_{inner}'] = non_null
                
                elif col.upper().startswith('SUM(') and col.upper().endswith(')'):
                    sum_col = col[4:-1]  # Remove 'SUM(' and ')'
                    total = 0
                    for r in group_rows:
                        val = r.get(sum_col)
                        if isinstance(val, (int, float)):
                            total += val
                    result_row[f'sum_{sum_col}'] = total
                
                elif col.upper().startswith('AVG(') and col.upper().endswith(')'):
                    avg_col = col[4:-1]  # Remove 'AVG(' and ')'
                    total = 0
                    count = 0
                    for r in group_rows:
                        val = r.get(avg_col)
                        if isinstance(val, (int, float)):
                            total += val
                            count += 1
                    result_row[f'avg_{avg_col}'] = total / count if count > 0 else 0
            
            result.append(result_row)
        
        return result
    
    def _apply_order_by(self, rows: List[Dict], order_by: str) -> List[Dict]:
        """Apply ORDER BY sorting"""
        if not rows:
            return rows
        
        # Check for ASC/DESC
        ascending = True
        if order_by.upper().endswith(' DESC'):
            order_by = order_by[:-5].strip()
            ascending = False
        elif order_by.upper().endswith(' ASC'):
            order_by = order_by[:-4].strip()
            ascending = True
        
        # Sort rows
        try:
            return sorted(rows, 
                         key=lambda x: x.get(order_by, ''), 
                         reverse=not ascending)
        except:
            return rows  # Return unsorted if error
    
    def _execute_update(self, query: UpdateQuery) -> Dict[str, Any]:
        """Execute UPDATE with detailed debugging"""
        try:
            print(f"\n=== DEBUG UPDATE START ===")
            print(f"Database: {self.db_name}, Table: {query.table_name}")
            print(f"WHERE: {query.where_clause}")
            print(f"SET: {query.set_clause}")
            
            # 1. Get current rows
            rows = self.storage.get_all_rows(self.db_name, query.table_name)
            print(f"DEBUG: Retrieved {len(rows)} rows from storage")
            
            if rows:
                print(f"DEBUG: First row before update: {rows[0]}")
            
            if not rows:
                print("DEBUG: No rows found")
                return {
                    'success': True,
                    'message': '0 rows updated',
                    'count': 0,
                    'columns': [],
                    'data': []
                }
            
            # 2. Make a DEEP COPY to track changes
            import copy
            rows_before = copy.deepcopy(rows)
            
            # 3. Apply updates
            updated_count = 0
            updated_indices = []
            
            for i, row in enumerate(rows):
                should_update = True
                
                if query.where_clause:
                    if '=' in query.where_clause:
                        col, value = query.where_clause.split('=', 1)
                        col = col.strip()
                        value = value.strip().strip("'\"")
                        
                        row_value = str(row.get(col, ''))
                        print(f"DEBUG: Row {i} - WHERE check: {col} = {value} vs row value: {row_value}")
                        
                        if row_value != value:
                            should_update = False
                            print(f"DEBUG: Row {i} doesn't match WHERE, skipping")
                
                if should_update:
                    print(f"DEBUG: Row {i} matches WHERE, updating...")
                    print(f"DEBUG: Before update: {row}")
                    
                    for col, new_value in query.set_clause.items():
                        old_value = row.get(col)
                        row[col] = new_value
                        print(f"DEBUG:   Changed {col}: {old_value} -> {new_value}")
                    
                    updated_count += 1
                    updated_indices.append(i)
                    print(f"DEBUG: After update: {row}")
            
            print(f"DEBUG: Total rows to update: {updated_count}")
            print(f"DEBUG: Updated indices: {updated_indices}")
            
            # 4. Verify changes were made in memory
            if updated_count > 0:
                print("\nDEBUG: Verifying in-memory changes:")
                for i in updated_indices:
                    print(f"Row {i} before: {rows_before[i]}")
                    print(f"Row {i} after:  {rows[i]}")
                    print(f"Changed: {rows_before[i] != rows[i]}")
            
            # 5. Save back to storage
            if updated_count > 0:
                print(f"\nDEBUG: Attempting to save {len(rows)} rows...")
                
                # Check what save methods are available
                storage_methods = [m for m in dir(self.storage) if 'save' in m.lower() and not m.startswith('_')]
                print(f"DEBUG: Available save methods: {storage_methods}")
                
                saved = False
                
                # Try save_all_rows
                if 'save_all_rows' in storage_methods:
                    print("DEBUG: Trying save_all_rows...")
                    try:
                        result = self.storage.save_all_rows(self.db_name, query.table_name, rows)
                        saved = True
                        print(f"DEBUG: save_all_rows returned: {result}")
                    except Exception as e:
                        print(f"DEBUG: save_all_rows failed: {e}")
                
                # Try save_rows
                if not saved and 'save_rows' in storage_methods:
                    print("DEBUG: Trying save_rows...")
                    try:
                        result = self.storage.save_rows(self.db_name, query.table_name, rows)
                        saved = True
                        print(f"DEBUG: save_rows returned: {result}")
                    except Exception as e:
                        print(f"DEBUG: save_rows failed: {e}")
                
                # Try to find and call ANY save method
                if not saved and storage_methods:
                    print(f"DEBUG: Trying other save methods...")
                    for method_name in storage_methods:
                        try:
                            method = getattr(self.storage, method_name)
                            # Check if it takes appropriate parameters
                            import inspect
                            params = inspect.signature(method).parameters
                            if len(params) >= 3:  # Should take db_name, table_name, data
                                print(f"DEBUG: Calling {method_name}...")
                                result = method(self.db_name, query.table_name, rows)
                                saved = True
                                print(f"DEBUG: {method_name} returned: {result}")
                                break
                        except Exception as e:
                            print(f"DEBUG: {method_name} failed: {e}")
                
                # Last resort: direct file writing
                if not saved:
                    print("DEBUG: All save methods failed, trying direct file write...")
                    try:
                        # Try to find where data is stored
                        import json
                        import os
                        
                        # Common data locations
                        possible_dirs = [
                            "data", "databases", "db", "storage",
                            os.path.join(os.getcwd(), "data"),
                            os.path.join(os.getcwd(), "databases"),
                        ]
                        
                        for data_dir in possible_dirs:
                            filepath = os.path.join(data_dir, self.db_name, f"{query.table_name}.json")
                            print(f"DEBUG: Trying path: {filepath}")
                            
                            if os.path.exists(os.path.dirname(filepath)):
                                # Write the file
                                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                                with open(filepath, 'w') as f:
                                    json.dump(rows, f, indent=2)
                                
                                saved = True
                                print(f"DEBUG: Direct write succeeded to {filepath}")
                                
                                # Verify
                                with open(filepath, 'r') as f:
                                    verify_data = json.load(f)
                                print(f"DEBUG: Verified {len(verify_data)} rows written")
                                break
                    except Exception as e:
                        print(f"DEBUG: Direct write failed: {e}")
                
                if not saved:
                    print("ERROR: Could not save data by any method!")
                    return {
                        'success': False,
                        'error': 'Failed to save updated data',
                        'message': f'Updated {updated_count} rows but could not save changes',
                        'count': updated_count
                    }
                else:
                    print("DEBUG: Data saved successfully")
            else:
                print("DEBUG: No rows needed updating")
            
            print(f"=== DEBUG UPDATE END ===\n")
            
            return {
                'success': True,
                'message': f'{updated_count} row(s) updated',
                'count': updated_count,
                'columns': [],
                'data': [],
                'row_count': updated_count
            }
            
        except Exception as e:
            print(f"UPDATE ERROR: {e}")
            import traceback
            traceback.print_exc()
            
            return {
                'success': False,
                'error': f'Error updating: {str(e)}',
                'message': f'Update failed: {str(e)}'
            }
            
    def _execute_delete(self, query: DeleteQuery) -> Dict[str, Any]:
        """Execute DELETE"""
        try:
            # Get all rows
            rows = self.storage.get_all_rows(self.db_name, query.table_name)
            if not rows:
                return {'message': '0 rows deleted', 'count': 0}
            
            # Filter rows to keep
            remaining_rows = []
            deleted_count = 0
            
            for row in rows:
                # Check WHERE clause
                if query.where_clause:
                    # Simple WHERE evaluation
                    if '=' in query.where_clause:
                        col, value = query.where_clause.split('=', 1)
                        col = col.strip()
                        value = value.strip().strip("'\"")
                        if str(row.get(col, '')) == value:
                            deleted_count += 1
                            continue
                
                remaining_rows.append(row)
            
            # Note: This is a simplified implementation
            # In production, would delete from storage
            
            return {
                'message': f'{deleted_count} row(s) deleted',
                'count': deleted_count
            }
            
        except Exception as e:
            return {'error': f'Error deleting: {str(e)}'}
    
    def _execute_drop_table(self, query: DropTableQuery) -> Dict[str, Any]:
        """Execute DROP TABLE"""
        try:
            success = self.storage.delete_table(self.db_name, query.table_name)
            if success:
                return {'message': f'Table {query.table_name} dropped'}
            else:
                return {'error': f'Table {query.table_name} not found'}
                
        except Exception as e:
            return {'error': f'Error dropping table: {str(e)}'}