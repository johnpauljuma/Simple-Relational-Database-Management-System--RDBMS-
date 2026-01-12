from flask import Flask, jsonify, request
from flask_cors import CORS
from engine.database import Database
from engine.storage import Storage
import os

app = Flask(__name__)
CORS(app)  # Enable CORS for frontend

storage = Storage()

@app.route('/api/databases', methods=['GET'])
def list_databases():
    """Get list of all databases"""
    databases = storage.list_databases()
    return jsonify({'databases': databases})

@app.route('/api/databases', methods=['POST'])
def create_database():
    """Create a new database"""
    data = request.json
    db_name = data.get('name')
    
    if not db_name:
        return jsonify({'error': 'Database name required'}), 400
    
    success = storage.create_database(db_name)
    if success:
        return jsonify({'message': f'Database {db_name} created'})
    else:
        return jsonify({'error': f'Database {db_name} already exists'}), 400

@app.route('/api/databases/<db_name>/tables', methods=['GET'])
def list_tables(db_name):
    """List tables in a database"""
    meta = storage._load_metadata(db_name)
    return jsonify({'tables': meta.get('tables', [])})

@app.route('/api/databases/<db_name>/execute', methods=['POST'])
def execute_query(db_name):
    """Execute a query on a database"""
    data = request.json
    query = data.get('query', '')
    
    if not query:
        return jsonify({'error': 'Query required'}), 400
    
    # Check if database exists
    if not os.path.exists(os.path.join(storage.data_dir, db_name)):
        return jsonify({'error': f'Database {db_name} not found'}), 404
    
    # Execute query
    db = Database(db_name)
    result = db.execute(query)
    
    return jsonify(result)

@app.route('/api/databases/<db_name>/tables/<table_name>/data', methods=['GET'])
def get_table_data(db_name, table_name):
    """Get all data from a table"""
    rows = storage.get_all_rows(db_name, table_name)
    schema = storage.load_table_schema(db_name, table_name)
    
    return jsonify({
        'schema': schema,
        'rows': rows,
        'count': len(rows)
    })

@app.route('/api/databases/<db_name>/tables/<table_name>/schema', methods=['GET'])
def get_table_schema(db_name, table_name):
    """Get table schema"""
    schema = storage.load_table_schema(db_name, table_name)
    return jsonify(schema)

if __name__ == '__main__':
    app.run(debug=True, port=5000)