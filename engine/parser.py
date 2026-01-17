"""
SQL Parser for MyRDBMS
Separated from database.py for better architecture
"""

import re
from typing import Dict, List, Any, Tuple, Optional
from dataclasses import dataclass
from engine.types import DataType, ConstraintType, ColumnDefinition
from engine.errors import ParseError

@dataclass
class ParsedQuery:
    """Base class for parsed queries"""
    query_type: str

@dataclass
class CreateTableQuery(ParsedQuery):
    """Parsed CREATE TABLE query"""
    table_name: str
    columns: List[ColumnDefinition]

@dataclass
class InsertQuery(ParsedQuery):
    """Parsed INSERT query"""
    table_name: str
    values: List[Any]

@dataclass
class SelectQuery(ParsedQuery):
    """Parsed SELECT query"""
    columns: List[str]
    table_name: str
    where_clause: Optional[str] = None
    join_clause: Optional[Dict] = None
    group_by: Optional[str] = None
    order_by: Optional[str] = None
    limit: Optional[int] = None

@dataclass  
class DropTableQuery(ParsedQuery):
    """Parsed DROP TABLE query"""
    table_name: str

@dataclass
class UpdateQuery(ParsedQuery):
    """Parsed UPDATE query"""
    table_name: str
    set_clause: Dict[str, Any]
    where_clause: Optional[str] = None

@dataclass
class DeleteQuery(ParsedQuery):
    """Parsed DELETE query"""
    table_name: str
    where_clause: Optional[str] = None

class SQLParser:
    """SQL Parser with proper separation of concerns"""
    
    @staticmethod
    def parse(query: str) -> ParsedQuery:
        """Main parsing method - routes to specific parsers"""
        query = query.strip()
        query_upper = query.upper()
        
        if query_upper.startswith('CREATE TABLE'):
            return SQLParser._parse_create_table(query)
        elif query_upper.startswith('INSERT INTO'):
            return SQLParser._parse_insert(query)
        elif query_upper.startswith('SELECT'):
            return SQLParser._parse_select(query)
        elif query_upper.startswith('UPDATE'):
            return SQLParser._parse_update(query)
        elif query_upper.startswith('DELETE FROM'):
            return SQLParser._parse_delete(query)
        elif query_upper.startswith('DROP TABLE'):
            return SQLParser._parse_drop_table(query)
        else:
            raise ParseError(f"Unsupported query type: {query}")
    
    @staticmethod
    def _parse_create_table(query: str) -> CreateTableQuery:
        """Parse CREATE TABLE query"""
        # Extract table name and columns
        pattern = r'CREATE TABLE\s+(\w+)\s*\((.*)\)'
        match = re.match(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise ParseError("Invalid CREATE TABLE syntax")
        
        table_name = match.group(1).strip()
        columns_text = match.group(2).strip()
        
        # Parse columns
        columns = []
        for col_def in re.split(r',\s*(?![^()]*\))', columns_text):
            col_def = col_def.strip()
            if not col_def:
                continue
                
            # Parse column definition
            parts = col_def.split()
            col_name = parts[0]
            data_type_str = parts[1] if len(parts) > 1 else 'TEXT'
            
            # Extract max length for VARCHAR
            max_length = None
            if '(' in data_type_str and ')' in data_type_str:
                # Handle types like VARCHAR(50)
                type_parts = data_type_str.split('(')
                base_type = type_parts[0].upper()
                length_str = type_parts[1].rstrip(')')
                try:
                    max_length = int(length_str)
                except:
                    max_length = None
                data_type_str = base_type
            else:
                base_type = data_type_str.upper()
            
            # Parse constraints
            constraints_list = []
            constraint_strs = parts[2:] if len(parts) > 2 else []
            
            # Check for constraint keywords
            constraints_combined = ' '.join(constraint_strs).upper()
            if 'PRIMARY' in constraints_combined and 'KEY' in constraints_combined:
                constraints_list.append(ConstraintType.PRIMARY_KEY)
            if 'UNIQUE' in constraints_combined:
                constraints_list.append(ConstraintType.UNIQUE)
            if 'NOT' in constraints_combined and 'NULL' in constraints_combined:
                constraints_list.append(ConstraintType.NOT_NULL)
            
            # Convert string to DataType enum
            try:
                data_type_enum = DataType(data_type_str.upper())
            except ValueError:
                data_type_enum = DataType.TEXT  # Default
            
            # Create ColumnDefinition
            column_def = ColumnDefinition(
                name=col_name,
                data_type=data_type_enum,
                constraints=constraints_list,
                max_length=max_length
            )
            
            columns.append(column_def)
        
        return CreateTableQuery(
            query_type='CREATE_TABLE',
            table_name=table_name,
            columns=columns
        )
    
    @staticmethod
    def _parse_insert(query: str) -> InsertQuery:
        """Parse INSERT query"""
        pattern = r'INSERT INTO\s+(\w+)\s+VALUES\s*\((.*)\)'
        match = re.match(pattern, query, re.IGNORECASE | re.DOTALL)
        
        if not match:
            raise ParseError("Invalid INSERT syntax")
        
        table_name = match.group(1).strip()
        values_text = match.group(2).strip()
        
        # Parse values
        values = []
        current = ''
        in_quotes = False
        paren_depth = 0
        
        for char in values_text:
            if char == "'" and not in_quotes:
                in_quotes = True
                current += char
            elif char == "'" and in_quotes:
                in_quotes = False
                current += char
            elif char == '(':
                paren_depth += 1
                current += char
            elif char == ')':
                paren_depth -= 1
                current += char
            elif char == ',' and not in_quotes and paren_depth == 0:
                values.append(SQLParser._parse_value(current.strip()))
                current = ''
            else:
                current += char
        
        if current:
            values.append(SQLParser._parse_value(current.strip()))
        
        return InsertQuery(
            query_type='INSERT',
            table_name=table_name,
            values=values
        )
    
    @staticmethod
    def _parse_value(value_str: str) -> Any:
        """Parse a single SQL value"""
        if not value_str or value_str.upper() == 'NULL':
            return None
        elif value_str.startswith("'") and value_str.endswith("'"):
            return value_str[1:-1]  # Remove quotes
        elif value_str.upper() == 'TRUE':
            return True
        elif value_str.upper() == 'FALSE':
            return False
        elif '.' in value_str:
            try:
                return float(value_str)
            except:
                return value_str
        else:
            try:
                return int(value_str)
            except:
                return value_str
    
    @staticmethod
    def _extract_max_length(data_type: str) -> Optional[int]:
        """Extract max length from data type like VARCHAR(50)"""
        match = re.match(r'(\w+)\((\d+)\)', data_type.upper())
        if match:
            try:
                return int(match.group(2))
            except:
                pass
        return None
    
    @staticmethod
    def _parse_select(query: str) -> SelectQuery:
        """Parse SELECT query with JOIN, WHERE, GROUP BY support"""
        query = re.sub(r'\s+', ' ', query).strip()
        
        # Extract SELECT columns
        select_match = re.match(r'SELECT\s+(.+?)\s+FROM', query, re.IGNORECASE)
        if not select_match:
            raise ParseError("Invalid SELECT syntax")
        
        columns_part = select_match.group(1).strip()
        columns = ['*'] if columns_part == '*' else [c.strip() for c in columns_part.split(',')]
        
        # Extract FROM and beyond
        from_part = query[select_match.end():].strip()
        
        # Parse table name (handle aliases)
        table_name = None
        table_alias = None
        join_clause = None
        where_clause = None
        group_by = None
        order_by = None
        limit = None
        
        # Simple parser - split by spaces
        words = from_part.split()
        
        if words:
            # Get main table (could have alias)
            table_with_alias = words[0]
            if ' ' in table_with_alias:
                # Has alias like "invoices i"
                table_parts = table_with_alias.split()
                table_name = table_parts[0]
                table_alias = table_parts[1] if len(table_parts) > 1 else None
            else:
                table_name = table_with_alias
                table_alias = None
        
        # Look for JOIN
        join_keywords = ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN']
        join_found = False
        join_type = 'INNER'
        join_table = None
        join_alias = None
        on_clause = None
        
        for i, word in enumerate(words):
            upper_word = word.upper()
            
            if upper_word in join_keywords:
                join_found = True
                join_type = 'INNER' if upper_word == 'JOIN' else upper_word.split()[0]
                
                # Get join table (next word, could have alias)
                if i + 1 < len(words):
                    join_table_with_alias = words[i + 1]
                    if ' ' in join_table_with_alias:
                        join_parts = join_table_with_alias.split()
                        join_table = join_parts[0]
                        join_alias = join_parts[1] if len(join_parts) > 1 else None
                    else:
                        join_table = join_table_with_alias
                        join_alias = None
                
                # Look for ON clause
                for j in range(i + 2, len(words)):
                    if words[j].upper() == 'ON':
                        # Collect ON clause until WHERE/ORDER/GROUP/LIMIT
                        on_tokens = []
                        for k in range(j + 1, len(words)):
                            if words[k].upper() in ['WHERE', 'ORDER', 'GROUP', 'LIMIT']:
                                break
                            on_tokens.append(words[k])
                        on_clause = ' '.join(on_tokens)
                        break
                
                break
        
        if join_found and join_table:
            join_clause = {
                'type': join_type,
                'table': join_table,
                'alias': join_alias,
                'on': on_clause
            }
        
        # Look for WHERE (simplified - needs improvement for complex WHERE with JOIN)
        if 'WHERE' in [w.upper() for w in words]:
            where_idx = [w.upper() for w in words].index('WHERE')
            where_end = len(words)
            
            # Find end of WHERE clause
            for clause in ['GROUP', 'ORDER', 'LIMIT']:
                if clause in [w.upper() for w in words[where_idx:]]:
                    clause_idx = [w.upper() for w in words[where_idx:]].index(clause)
                    where_end = where_idx + clause_idx
                    break
            
            where_clause = ' '.join(words[where_idx + 1:where_end])
        
        # Look for GROUP BY
        if 'GROUP BY' in from_part.upper():
            group_match = re.search(r'GROUP BY\s+(\w+)', from_part, re.IGNORECASE)
            if group_match:
                group_by = group_match.group(1)
        
        # Look for ORDER BY
        if 'ORDER BY' in from_part.upper():
            order_match = re.search(r'ORDER BY\s+(.+?)(?:\s+(?:LIMIT|$))', from_part, re.IGNORECASE)
            if order_match:
                order_by = order_match.group(1).strip()
        
        # Look for LIMIT
        if 'LIMIT' in from_part.upper():
            limit_match = re.search(r'LIMIT\s+(\d+)', from_part, re.IGNORECASE)
            if limit_match:
                try:
                    limit = int(limit_match.group(1))
                except:
                    limit = None
        
        return SelectQuery(
            query_type='SELECT',
            columns=columns,
            table_name=table_name,  # Just the table name, no alias
            where_clause=where_clause,
            join_clause=join_clause,
            group_by=group_by,
            order_by=order_by,
            limit=limit
        )
    
    @staticmethod
    def _parse_update(query: str) -> UpdateQuery:
        """Parse UPDATE query"""
        pattern = r'UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid UPDATE syntax")
        
        table_name = match.group(1).strip()
        set_clause = match.group(2).strip()
        where_clause = match.group(3).strip() if match.group(3) else None
        
        # Parse SET clause
        set_pairs = {}
        for pair in set_clause.split(','):
            pair = pair.strip()
            if '=' in pair:
                col, value = pair.split('=', 1)
                set_pairs[col.strip()] = SQLParser._parse_value(value.strip())
        
        return UpdateQuery(
            query_type='UPDATE',
            table_name=table_name,
            set_clause=set_pairs,
            where_clause=where_clause
        )
    
    @staticmethod
    def _parse_delete(query: str) -> DeleteQuery:
        """Parse DELETE query"""
        pattern = r'DELETE FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid DELETE syntax")
        
        table_name = match.group(1).strip()
        where_clause = match.group(2).strip() if match.group(2) else None
        
        return DeleteQuery(
            query_type='DELETE',
            table_name=table_name,
            where_clause=where_clause
        )
    
    @staticmethod
    def _parse_drop_table(query: str) -> DropTableQuery:
        """Parse DROP TABLE query"""
        pattern = r'DROP TABLE\s+(\w+)'
        match = re.match(pattern, query, re.IGNORECASE)
        
        if not match:
            raise ParseError("Invalid DROP TABLE syntax")
        
        return DropTableQuery(
            query_type='DROP_TABLE',
            table_name=match.group(1).strip()
        )
    
    @staticmethod
    def validate_query(query: str) -> bool:
        """Basic query validation"""
        query = query.strip().upper()
        
        # Check for basic SQL structure
        if not query:
            return False
        
        # Check for required keywords based on query type
        if query.startswith('CREATE TABLE'):
            if '(' not in query or ')' not in query:
                return False
        elif query.startswith('INSERT INTO'):
            if 'VALUES' not in query or '(' not in query or ')' not in query:
                return False
        elif query.startswith('SELECT'):
            if 'FROM' not in query:
                return False
        elif query.startswith('UPDATE'):
            if 'SET' not in query:
                return False
        elif query.startswith('DELETE FROM'):
            pass  # DELETE FROM is valid
        elif query.startswith('DROP TABLE'):
            pass  # DROP TABLE is valid
        
        return True
    
    @staticmethod
    def get_query_type(query: str) -> str:
        """Get the type of SQL query"""
        query_upper = query.strip().upper()
        
        if query_upper.startswith('CREATE TABLE'):
            return 'CREATE_TABLE'
        elif query_upper.startswith('INSERT INTO'):
            return 'INSERT'
        elif query_upper.startswith('SELECT'):
            return 'SELECT'
        elif query_upper.startswith('UPDATE'):
            return 'UPDATE'
        elif query_upper.startswith('DELETE FROM'):
            return 'DELETE'
        elif query_upper.startswith('DROP TABLE'):
            return 'DROP_TABLE'
        else:
            return 'UNKNOWN'