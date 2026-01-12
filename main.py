#!/usr/bin/env python3
"""
MyRDBMS - A simple relational database management system
Run this file to start the database server and web interface
"""

import os
import sys
import webbrowser
from threading import Thread
import time

def start_api_server():
    """Start the Flask API server"""
    from api.server import app
    print("Starting API server on http://localhost:5000")
    app.run(debug=True, port=5000, use_reloader=False)

def start_web_server():
    """Start a simple web server for the frontend"""
    import http.server
    import socketserver
    
    os.chdir('web-ui')  # Change to web-ui directory
    
    PORT = 3000
    Handler = http.server.SimpleHTTPRequestHandler
    
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print(f"Web UI available at http://localhost:{PORT}")
        webbrowser.open(f'http://localhost:{PORT}')
        httpd.serve_forever()

def main():
    """Main entry point"""
    print("""
    ╔══════════════════════════════════════════╗
    ║         MyRDBMS - Starting...            ║
    ╚══════════════════════════════════════════╝
    """)
    
    # Create necessary directories
    os.makedirs('data', exist_ok=True)
    os.makedirs('web-ui', exist_ok=True)
    
    # Check if web files exist
    if not os.path.exists('web-ui/index.html'):
        print("Warning: web-ui/index.html not found")
        print("Make sure you have the web-ui files in place")
    
    # Start API server in a separate thread
    api_thread = Thread(target=start_api_server, daemon=True)
    api_thread.start()
    
    # Give API server time to start
    time.sleep(2)
    
    # Start web server
    print("\n" + "="*50)
    print("MyRDBMS is running!")
    print("="*50)
    print("1. API Server: http://localhost:5000")
    print("2. Web Interface: http://localhost:3000")
    print("3. Try these queries:")
    print("   - CREATE TABLE users (id INT, name VARCHAR(50))")
    print("   - INSERT INTO users VALUES (1, 'John')")
    print("   - SELECT * FROM users")
    print("="*50 + "\n")
    
    try:
        start_web_server()
    except KeyboardInterrupt:
        print("\nShutting down MyRDBMS...")
        sys.exit(0)

if __name__ == '__main__':
    main()