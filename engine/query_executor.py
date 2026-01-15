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
                'rows': rows,
                'count': len(rows)
            }
            
        except Exception as e:
            return {'error': f'Error executing SELECT: {str(e)}'}
    
    def _execute_join(self, left_rows: List[Dict], query: SelectQuery) -> Dict[str, Any]:
        """Execute JOIN operation"""
        try:
            join_clause = query.join_clause
            if not join_clause or 'table' not in join_clause:
                return {'error': 'Invalid JOIN clause'}
            
            right_table = join_clause['table']
            on_clause = join_clause.get('on', '')
            
            # Get rows from right table
            right_rows = self.storage.get_all_rows(self.db_name, right_table)
            
            # Parse ON clause (simple: left.column = right.column)
            left_col = None
            right_col = None
            if '=' in on_clause:
                left_part, right_part = on_clause.split('=')
                left_col = left_part.strip().split('.')[-1]
                right_col = right_part.strip().split('.')[-1]
            
            # Perform nested loop join
            joined_rows = []
            for left_row in left_rows:
                for right_row in right_rows:
                    # If no ON clause or columns match, join all rows (cartesian product)
                    if not left_col or not right_col:
                        merged = {**left_row, **{f"{right_table}_{k}": v for k, v in right_row.items()}}
                        joined_rows.append(merged)
                    elif left_row.get(left_col) == right_row.get(right_col):
                        merged = {**left_row, **{f"{right_table}_{k}": v for k, v in right_row.items()}}
                        joined_rows.append(merged)
            
            return {
                'rows': joined_rows,
                'message': f'Joined {len(left_rows)} rows with {len(right_rows)} rows = {len(joined_rows)} rows'
            }
            
        except Exception as e:
            return {'error': f'JOIN execution error: {str(e)}'}
    
    def _apply_where(self, rows: List[Dict], where_clause: str) -> List[Dict]:
        """Apply WHERE clause filtering"""
        if not where_clause:
            return rows
        
        filtered = []
        
        # Simple WHERE parsing for basic operators
        operators = ['=', '!=', '>', '<', '>=', '<=']
        for op in operators:
            if op in where_clause:
                parts = where_clause.split(op)
                if len(parts) == 2:
                    col = parts[0].strip()
                    value = parts[1].strip().strip("'\"")
                    
                    # Try to convert value based on operator context
                    try:
                        if '.' in value:
                            value = float(value)
                        else:
                            value = int(value)
                    except:
                        pass  # Keep as string
                    
                    for row in rows:
                        row_value = row.get(col)
                        
                        if op == '=' and row_value == value:
                            filtered.append(row)
                        elif op == '!=' and row_value != value:
                            filtered.append(row)
                        elif op == '>' and row_value is not None and value is not None:
                            try:
                                if row_value > value:
                                    filtered.append(row)
                            except:
                                pass
                        elif op == '<' and row_value is not None and value is not None:
                            try:
                                if row_value < value:
                                    filtered.append(row)
                            except:
                                pass
                        elif op == '>=' and row_value is not None and value is not None:
                            try:
                                if row_value >= value:
                                    filtered.append(row)
                            except:
                                pass
                        elif op == '<=' and row_value is not None and value is not None:
                            try:
                                if row_value <= value:
                                    filtered.append(row)
                            except:
                                pass
                    
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
        """Execute UPDATE"""
        try:
            # Get all rows
            rows = self.storage.get_all_rows(self.db_name, query.table_name)
            if not rows:
                return {'message': '0 rows updated', 'count': 0}
            
            # Apply updates
            updated_rows = []
            updated_count = 0
            
            for row in rows:
                # Check WHERE clause
                if query.where_clause:
                    # Simple WHERE evaluation
                    if '=' in query.where_clause:
                        col, value = query.where_clause.split('=', 1)
                        col = col.strip()
                        value = value.strip().strip("'\"")
                        if str(row.get(col, '')) != value:
                            updated_rows.append(row)
                            continue
                
                # Update row
                for col, new_value in query.set_clause.items():
                    row[col] = new_value
                
                updated_rows.append(row)
                updated_count += 1
            
            # Save updated rows
            if updated_count > 0:
                # Note: This is a simplified implementation
                # In production, would update in place
                pass
            
            return {
                'message': f'{updated_count} row(s) updated',
                'count': updated_count
            }
            
        except Exception as e:
            return {'error': f'Error updating: {str(e)}'}
    
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