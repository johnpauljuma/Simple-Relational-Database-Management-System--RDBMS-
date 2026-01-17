# SIMPLE RDBMS - Lightweight Relational Database Management System

## 🚀 Overview

SRDBMS is a lightweight, web-based relational database management system built with Python (Flask) and JavaScript. It provides a SQL-like interface for managing databases, tables, and data through an intuitive web interface.

## ✨ Features

### 📊 Database Management
- Create, list, and select databases
- Persistent JSON-based storage
- Multi-database support

### 📋 Table Operations
- Create tables with custom schemas
- Support for common data types (INT, VARCHAR, TEXT, BOOLEAN, DECIMAL, DATE)
- Define primary keys, unique constraints, and nullable fields
- View table schemas and relationships

### 🔍 SQL Query Interface
- Execute SQL queries (SELECT, INSERT, UPDATE, DELETE)
- Support for WHERE clauses
- Real-time query execution feedback
- Query history and results display

### 📱 Web-Based Interface
- Modern, responsive UI
- Tab-based navigation (Query, Data, Schema)
- Real-time data visualization
- Keyboard shortcuts (Ctrl+Enter to execute)

### 🔧 CRUD Operations
- Insert new rows with form-based input
- Update existing records
- Delete rows (with confirmation)
- View and edit table data

## 🏗️ Architecture

### Backend (Python/Flask)
- **Storage Layer**: JSON-based file storage
- **Query Parser**: SQL-like query parsing and execution
- **API Layer**: RESTful endpoints for all operations
- **Error Handling**: Comprehensive error handling and validation

### Frontend (JavaScript/HTML/CSS)
- **API Client**: Handles all backend communication
- **UI Components**: Modular, reusable components
- **State Management**: Client-side state for databases and tables
- **Real-time Updates**: Automatic refresh after modifications

## 📦 Installation

### Prerequisites
- Python 3.8+
- Flask
- Modern web browser

### Setup
```bash
# Clone the repository
git clone https://github.com/yourusername/myrdbms.git
cd myrdbms

# Install Python dependencies
pip install flask

# Run the application
python app.py
```

### File Structure
```
myrdbms/
├── app.py              # Main Flask application
├── storage/           # Storage layer implementation
├── query/             # Query parser and executor
├── static/
│   ├── css/          # Stylesheets
│   ├── js/           # JavaScript files
│   └── index.html    # Main HTML file
├── data/             # Database storage directory
└── README.md         # This file
```

## 🔌 API Endpoints

### Database Operations
- `GET /api/databases` - List all databases
- `POST /api/databases` - Create new database
- `GET /api/databases/{db_name}/tables` - List tables in database

### Table Operations
- `GET /api/databases/{db_name}/tables/{table_name}/schema` - Get table schema
- `GET /api/databases/{db_name}/tables/{table_name}/data` - Get table data

### Query Execution
- `POST /api/databases/{db_name}/execute` - Execute SQL query

## 📖 SQL Support

### Supported Statements
- **CREATE TABLE**: Define new tables with columns and constraints
- **SELECT**: Retrieve data with optional WHERE clauses
- **INSERT**: Add new rows to tables
- **UPDATE**: Modify existing rows with WHERE conditions
- **DELETE**: Remove rows with WHERE conditions

### Data Types
- `INT` - Integer numbers
- `VARCHAR(n)` - Variable-length strings
- `TEXT` - Long text
- `BOOLEAN` - True/False values
- `DECIMAL` - Decimal numbers
- `DATE` - Date values

### Constraints
- `PRIMARY KEY` - Unique identifier for rows
- `UNIQUE` - Unique values in column
- `NOT NULL` - Column cannot be empty

## 🎯 Usage Examples

### Creating a Database
```sql
-- Using the web interface or API
CREATE DATABASE invoicing_db;
```

### Creating a Table
```sql
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE,
    created_at DATE
);
```

### Basic Operations
```sql
-- Insert data
INSERT INTO customers VALUES (1, 'John Doe', 'john@example.com', '2024-01-01');

-- Query data
SELECT * FROM customers WHERE id = 1;

-- Update data
UPDATE customers SET name = 'Jane Doe' WHERE id = 1;

-- Delete data
DELETE FROM customers WHERE id = 1;
```

## 🎨 Web Interface Guide

### Query Tab
- **Query Editor**: Write and execute SQL queries
- **Results Panel**: View query results and execution time
- **History**: Recent queries and their outcomes

### Data Tab
- **Table View**: Browse table data in tabular format
- **CRUD Buttons**: Insert, edit, and delete rows
- **Search/Filter**: Filter data by column values

### Schema Tab
- **Schema Viewer**: View table structure and constraints
- **Column Details**: Data types, constraints, and nullability
- **Relationships**: Foreign key relationships (planned)

## ⌨️ Keyboard Shortcuts

| Shortcut | Action |
|----------|---------|
| `Ctrl+Enter` | Execute current query |
| `Esc` | Clear query editor |
| `Ctrl+S` | Save current query (planned) |
| `Ctrl+E` | Expand/collapse results |

## 🔧 Development

### Running in Development Mode
```bash
# Enable debug mode
export FLASK_ENV=development
python app.py
```

### Adding New Features
1. Extend the query parser in `query/parser.py`
2. Add new API endpoints in `app.py`
3. Update frontend components in `static/js/`

### Testing
```bash
# Run unit tests (planned)
python -m pytest tests/
```

## 🚧 Known Limitations

1. **WHERE Clause**: Currently only supports simple equality comparisons
2. **Transactions**: No transaction support or rollback capability
3. **Concurrency**: Single-user access only
4. **Performance**: Not optimized for large datasets
5. **Security**: Basic authentication planned for future releases

## 📈 Roadmap

### Phase 1 (Current)
- ✅ Basic CRUD operations
- ✅ Web interface
- ✅ SQL-like query language
- ✅ JSON storage backend

### Phase 2 (In Progress)
- 🔄 Improved WHERE clause support
- 🔄 JOIN operations
- 🔄 Indexing for performance
- 🔄 Export/import functionality

### Phase 3 (Planned)
- 📅 User authentication
- 📅 Role-based access control
- 📅 Transaction support
- 📅 Database backups
- 📅 Advanced query optimization

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Guidelines
- Follow PEP 8 for Python code
- Use meaningful commit messages
- Add tests for new features
- Update documentation accordingly

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgments

- Flask team for the excellent web framework
- Bootstrap for the CSS framework
- Font Awesome for icons
- All contributors and testers

## 📞 Support

For issues, questions, or feature requests:
1. Contact: jumajohnpa@gmail.com

---

**MyRDBMS** - Making database management accessible and intuitive. 🗄️✨

---

*Last Updated: January 2026*  
*Version: 1.0.0*
