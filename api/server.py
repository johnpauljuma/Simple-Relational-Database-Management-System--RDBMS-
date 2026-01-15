"""
MyRDBMS REST API Server
Provides HTTP interface to the database engine
"""

from flask import Flask, jsonify, request
from flask_cors import CORS
from engine import Database, Storage
import os
import traceback

app = Flask(__name__)
CORS(app)

# Initialize storage
storage = Storage()

# ==================== DATABASE ENDPOINTS ====================

@app.route('/api/databases', methods=['GET'])
def list_databases():
    """Get list of all databases"""
    databases = storage.list_databases()
    return jsonify({
        'success': True,
        'databases': databases, 
        'count': len(databases)
    })

@app.route('/api/databases', methods=['POST'])
def create_database():
    """Create a new database"""
    data = request.json
    db_name = data.get('name') if data else None
    
    if not db_name:
        return jsonify({
            'success': False,
            'error': 'Database name required'
        }), 400
    
    success = storage.create_database(db_name)
    if success:
        return jsonify({
            'success': True,
            'message': f'Database {db_name} created'
        })
    else:
        return jsonify({
            'success': False,
            'error': f'Database {db_name} already exists'
        }), 400

@app.route('/api/databases/<db_name>', methods=['DELETE'])
def delete_database(db_name):
    """Delete a database (and all its tables)"""
    try:
        db_path = os.path.join(storage.data_dir, db_name)
        if os.path.exists(db_path):
            import shutil
            shutil.rmtree(db_path)
            return jsonify({
                'success': True,
                'message': f'Database {db_name} deleted'
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Database {db_name} not found'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error deleting database: {str(e)}'
        }), 500

# ==================== TABLE ENDPOINTS ====================

@app.route('/api/databases/<db_name>/tables', methods=['GET'])
def list_tables(db_name):
    """List tables in a database"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    meta = storage._load_metadata(db_name)
    return jsonify({
        'success': True,
        'tables': meta.get('tables', []),
        'count': len(meta.get('tables', []))
    })

@app.route('/api/databases/<db_name>/tables/<table_name>', methods=['GET'])
def get_table_info(db_name, table_name):
    """Get table information (schema + stats)"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    if not storage.table_exists(db_name, table_name):
        return jsonify({
            'success': False,
            'error': f'Table {table_name} not found in database {db_name}'
        }), 404
    
    # Get schema
    schema = storage.load_table_schema(db_name, table_name)
    
    # Get data stats
    rows = storage.get_all_rows(db_name, table_name)
    
    # Get indexes (if any)
    # In production, would get from IndexManager
    
    return jsonify({
        'success': True,
        'table': table_name,
        'schema': schema,
        'stats': {
            'row_count': len(rows),
            'columns': len(schema.get('columns', [])) if schema else 0
        }
    })

@app.route('/api/databases/<db_name>/tables/<table_name>/schema', methods=['GET'])
def get_table_schema(db_name, table_name):
    """Get table schema"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    if not storage.table_exists(db_name, table_name):
        return jsonify({
            'success': False,
            'error': f'Table {table_name} not found'
        }), 404
    
    schema = storage.load_table_schema(db_name, table_name)
    return jsonify({
        'success': True,
        'schema': schema
    })

@app.route('/api/databases/<db_name>/tables/<table_name>/data', methods=['GET'])
def get_table_data(db_name, table_name):
    """Get all data from a table"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    if not storage.table_exists(db_name, table_name):
        return jsonify({
            'success': False,
            'error': f'Table {table_name} not found'
        }), 404
    
    rows = storage.get_all_rows(db_name, table_name)
    schema = storage.load_table_schema(db_name, table_name)
    
    return jsonify({
        'success': True,
        'schema': schema,
        'rows': rows,
        'count': len(rows)
    })

@app.route('/api/databases/<db_name>/tables/<table_name>', methods=['DELETE'])
def drop_table(db_name, table_name):
    """Drop (delete) a table"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    success = storage.delete_table(db_name, table_name)
    if success:
        return jsonify({
            'success': True,
            'message': f'Table {table_name} dropped'
        })
    else:
        return jsonify({
            'success': False,
            'error': f'Table {table_name} not found'
        }), 404

# ==================== QUERY EXECUTION ENDPOINTS ====================

@app.route('/api/databases/<db_name>/execute', methods=['POST'])
def execute_query(db_name):
    """Execute SQL query on database"""
    data = request.json
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    query = data.get('query', '')
    
    if not query:
        return jsonify({
            'success': False,
            'error': 'Query required'
        }), 400
    
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    try:
        # Create database instance and execute query
        db = Database(db_name)
        result = db.execute(query)
        
        # Ensure consistent response format
        if 'success' not in result:
            result['success'] = 'error' not in result
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Query execution error: {str(e)}',
            'traceback': traceback.format_exc() if app.debug else None
        }), 500

@app.route('/api/databases/<db_name>/execute/batch', methods=['POST'])
def execute_batch_queries(db_name):
    """Execute multiple SQL queries in batch"""
    data = request.json
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    queries = data.get('queries', [])
    
    if not queries or not isinstance(queries, list):
        return jsonify({
            'success': False,
            'error': 'List of queries required'
        }), 400
    
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    try:
        db = Database(db_name)
        results = []
        
        for query in queries:
            result = db.execute(query)
            results.append(result)
        
        return jsonify({
            'success': True,
            'results': results,
            'count': len(results)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Batch execution error: {str(e)}'
        }), 500

# ==================== DATABASE MANAGEMENT ENDPOINTS ====================

@app.route('/api/databases/<db_name>/stats', methods=['GET'])
def get_database_stats(db_name):
    """Get database statistics"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    try:
        db = Database(db_name)
        stats = db.get_stats()
        
        return jsonify({
            'success': True,
            'stats': stats
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error getting stats: {str(e)}'
        }), 500

@app.route('/api/databases/<db_name>/explain', methods=['POST'])
def explain_query(db_name):
    """Explain query execution plan"""
    data = request.json
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    query = data.get('query', '')
    
    if not query:
        return jsonify({
            'success': False,
            'error': 'Query required'
        }), 400
    
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    try:
        db = Database(db_name)
        plan = db.explain(query)
        
        return jsonify({
            'success': True,
            'plan': plan
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error explaining query: {str(e)}'
        }), 500

@app.route('/api/databases/<db_name>/indexes/<table_name>', methods=['GET'])
def list_table_indexes(db_name, table_name):
    """List indexes for a table"""
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    if not storage.table_exists(db_name, table_name):
        return jsonify({
            'success': False,
            'error': f'Table {table_name} not found'
        }), 404
    
    try:
        db = Database(db_name)
        indexes = db.list_indexes(table_name)
        
        return jsonify({
            'success': True,
            'indexes': indexes,
            'count': len(indexes)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error listing indexes: {str(e)}'
        }), 500

@app.route('/api/databases/<db_name>/indexes/<table_name>', methods=['POST'])
def create_table_index(db_name, table_name):
    """Create index on table column"""
    data = request.json
    if not data:
        return jsonify({
            'success': False,
            'error': 'Request body required'
        }), 400
    
    column = data.get('column')
    index_type = data.get('type', 'HASH')
    
    if not column:
        return jsonify({
            'success': False,
            'error': 'Column name required'
        }), 400
    
    if not storage.database_exists(db_name):
        return jsonify({
            'success': False,
            'error': f'Database {db_name} not found'
        }), 404
    
    if not storage.table_exists(db_name, table_name):
        return jsonify({
            'success': False,
            'error': f'Table {table_name} not found'
        }), 404
    
    try:
        db = Database(db_name)
        success = db.create_index(table_name, column, index_type)
        
        if success:
            return jsonify({
                'success': True,
                'message': f'Index created on {table_name}.{column}'
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Failed to create index'
            }), 500
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': f'Error creating index: {str(e)}'
        }), 500

# ==================== HEALTH & INFO ENDPOINTS ====================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Check if storage is accessible
        dbs = storage.list_databases()
        
        return jsonify({
            'status': 'healthy',
            'version': '1.0.0',
            'database_count': len(dbs),
            'storage_path': storage.data_dir,
            'storage_writable': os.access(storage.data_dir, os.W_OK) if os.path.exists(storage.data_dir) else False
        })
        
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500

@app.route('/api/info', methods=['GET'])
def api_info():
    """API information"""
    return jsonify({
        'name': 'MyRDBMS REST API',
        'version': '1.0.0',
        'description': 'RESTful API for MyRDBMS Database Engine',
        'endpoints': {
            'databases': {
                'GET /api/databases': 'List all databases',
                'POST /api/databases': 'Create new database',
                'DELETE /api/databases/<name>': 'Delete database',
                'GET /api/databases/<name>/stats': 'Get database statistics',
                'GET /api/databases/<name>/tables': 'List tables in database'
            },
            'tables': {
                'GET /api/databases/<db>/tables/<table>/schema': 'Get table schema',
                'GET /api/databases/<db>/tables/<table>/data': 'Get table data',
                'DELETE /api/databases/<db>/tables/<table>': 'Drop table'
            },
            'queries': {
                'POST /api/databases/<db>/execute': 'Execute SQL query',
                'POST /api/databases/<db>/execute/batch': 'Execute batch queries',
                'POST /api/databases/<db>/explain': 'Explain query plan'
            },
            'indexes': {
                'GET /api/databases/<db>/indexes/<table>': 'List table indexes',
                'POST /api/databases/<db>/indexes/<table>': 'Create index'
            },
            'system': {
                'GET /api/health': 'Health check',
                'GET /api/info': 'API information'
            }
        }
    })

@app.route('/api/debug', methods=['GET'])
def debug_info():
    """Debug information (only in debug mode)"""
    if not app.debug:
        return jsonify({
            'success': False,
            'error': 'Debug endpoint only available in debug mode'
        }), 403
    
    import sys
    import platform
    
    return jsonify({
        'python_version': sys.version,
        'platform': platform.platform(),
        'storage': {
            'data_dir': storage.data_dir,
            'exists': os.path.exists(storage.data_dir),
            'writable': os.access(storage.data_dir, os.W_OK) if os.path.exists(storage.data_dir) else False
        },
        'databases': storage.list_databases(),
        'flask_debug': app.debug
    })

# ==================== ERROR HANDLERS ====================

@app.errorhandler(404)
def not_found(error):
    """Handle 404 errors"""
    return jsonify({
        'success': False,
        'error': 'Endpoint not found'
    }), 404

@app.errorhandler(405)
def method_not_allowed(error):
    """Handle 405 errors"""
    return jsonify({
        'success': False,
        'error': 'Method not allowed'
    }), 405

@app.errorhandler(500)
def internal_error(error):
    """Handle 500 errors"""
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'traceback': traceback.format_exc() if app.debug else None
    }), 500

# ==================== MAIN ====================

if __name__ == '__main__':
    print("="*70)
    print("🚀 MyRDBMS API Server")
    print("="*70)
    print(f"📊 API URL: http://localhost:5000")
    print(f"🔧 Health check: http://localhost:5000/api/health")
    print(f"📚 API docs: http://localhost:5000/api/info")
    print(f"💾 Storage path: {storage.data_dir}")
    print("="*70)
    
    # Create default data directory if it doesn't exist
    os.makedirs(storage.data_dir, exist_ok=True)
    
    # Run the server
    app.run(debug=True, port=5000, host='0.0.0.0')