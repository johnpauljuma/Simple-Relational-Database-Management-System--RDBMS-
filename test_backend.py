# test_backend.py
import requests
import json

BASE_URL = "http://localhost:5000/api"

def test_backend():
    print("Testing MyRDBMS Backend...")
    print("="*50)
    
    # 1. Health check
    print("1. Health check...")
    response = requests.get(f"{BASE_URL}/health")
    print(f"   Status: {response.status_code}")
    print(f"   Response: {response.json()}")
    
    # 2. Create database
    print("\n2. Creating database...")
    response = requests.post(f"{BASE_URL}/databases", 
                           json={"name": "testdb"})
    print(f"   Response: {response.json()}")
    
    # 3. Create table
    print("\n3. Creating table...")
    query = """
    CREATE TABLE users (
        id INT PRIMARY KEY,
        name VARCHAR(50),
        email VARCHAR(100) UNIQUE,
        age INT
    )
    """
    response = requests.post(f"{BASE_URL}/databases/testdb/execute",
                           json={"query": query})
    print(f"   Response: {response.json()}")
    
    # 4. Insert data
    print("\n4. Inserting data...")
    queries = [
        "INSERT INTO users VALUES (1, 'John Doe', 'john@example.com', 30)",
        "INSERT INTO users VALUES (2, 'Jane Smith', 'jane@example.com', 25)"
    ]
    
    for query in queries:
        response = requests.post(f"{BASE_URL}/databases/testdb/execute",
                               json={"query": query})
        print(f"   {query}: {response.json()}")
    
    # 5. Select data
    print("\n5. Selecting data...")
    response = requests.post(f"{BASE_URL}/databases/testdb/execute",
                           json={"query": "SELECT * FROM users"})
    result = response.json()
    print(f"   Success: {result.get('success')}")
    print(f"   Message: {result.get('message')}")
    print(f"   Data: {result.get('data')}")
    print(f"   Columns: {result.get('columns')}")
    print(f"   Row count: {result.get('row_count')}")
    
    # 6. Get schema
    print("\n6. Getting schema...")
    response = requests.get(f"{BASE_URL}/databases/testdb/tables/users/schema")
    print(f"   Schema: {response.json()}")
    
    print("\n" + "="*50)
    print("Test complete!")

if __name__ == "__main__":
    test_backend()