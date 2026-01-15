"""
Join Executor for MyRDBMS
Handles various join operations
"""

from typing import Dict, List, Any

class JoinExecutor:
    """Executes JOIN operations between tables"""
    
    @staticmethod
    def nested_loop_join(left_rows: List[Dict], right_rows: List[Dict], 
                         on_clause: str) -> List[Dict]:
        """
        Perform nested loop join.
        on_clause format: "left_table.column = right_table.column"
        """
        # Parse ON clause
        left_part, right_part = on_clause.split('=')
        left_col = left_part.strip().split('.')[-1]
        right_col = right_part.strip().split('.')[-1]
        
        results = []
        
        for left_row in left_rows:
            for right_row in right_rows:
                if left_row.get(left_col) == right_row.get(right_col):
                    # Merge with prefix to avoid column name collisions
                    merged = left_row.copy()
                    for k, v in right_row.items():
                        if k in merged:
                            merged[f'right_{k}'] = v
                        else:
                            merged[k] = v
                    results.append(merged)
        
        return results
    
    @staticmethod
    def hash_join(left_rows: List[Dict], right_rows: List[Dict],
                  on_clause: str) -> List[Dict]:
        """
        Perform hash join (more efficient for larger datasets).
        Builds hash table from smaller table.
        """
        # Parse ON clause
        left_part, right_part = on_clause.split('=')
        left_col = left_part.strip().split('.')[-1]
        right_col = right_part.strip().split('.')[-1]
        
        # Build hash table from right table (assume smaller)
        hash_table = {}
        for right_row in right_rows:
            key = right_row.get(right_col)
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(right_row)
        
        # Probe with left table
        results = []
        for left_row in left_rows:
            key = left_row.get(left_col)
            if key in hash_table:
                for right_row in hash_table[key]:
                    merged = left_row.copy()
                    for k, v in right_row.items():
                        if k in merged:
                            merged[f'right_{k}'] = v
                        else:
                            merged[k] = v
                    results.append(merged)
        
        return results
    
    @staticmethod
    def left_outer_join(left_rows: List[Dict], right_rows: List[Dict],
                       on_clause: str) -> List[Dict]:
        """
        Perform LEFT OUTER JOIN.
        All rows from left table, NULLs for non-matching right rows.
        """
        # Parse ON clause
        left_part, right_part = on_clause.split('=')
        left_col = left_part.strip().split('.')[-1]
        right_col = right_part.strip().split('.')[-1]
        
        # Build hash table from right table
        hash_table = {}
        for right_row in right_rows:
            key = right_row.get(right_col)
            if key not in hash_table:
                hash_table[key] = []
            hash_table[key].append(right_row)
        
        # Probe with left table
        results = []
        for left_row in left_rows:
            key = left_row.get(left_col)
            if key in hash_table:
                for right_row in hash_table[key]:
                    merged = left_row.copy()
                    for k, v in right_row.items():
                        if k in merged:
                            merged[f'right_{k}'] = v
                        else:
                            merged[k] = v
                    results.append(merged)
            else:
                # No match - include left row with NULLs for right columns
                merged = left_row.copy()
                for k in right_rows[0].keys() if right_rows else []:
                    merged[k] = None
                results.append(merged)
        
        return results