// Add this for debugging
console.log("MyRDBMS Frontend loaded");
window.debugMode = true;

// Override fetch to log all API calls
const originalFetch = window.fetch;
window.fetch = async function(...args) {
    console.log('📡 API Call:', args[0], args[1]?.method || 'GET', args[1]?.body);
    const response = await originalFetch.apply(this, args);
    const clone = response.clone();
    try {
        const data = await clone.json();
        console.log('📡 API Response:', data);
    } catch (e) {
        console.log('📡 API Response (not JSON):', response.status, response.statusText);
    }
    return response;
};

// Global state
let currentDatabase = null;
let currentTable = null;
let activeDatabaseElement = null;
const API_BASE = 'http://localhost:5000/api';

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    console.log("MyRDBMS Frontend Initializing...");
    
    // Load databases
    loadDatabases();
    
    // Initialize tab buttons
    document.querySelectorAll('.tab').forEach(tab => {
        tab.addEventListener('click', function() {
            const tabName = this.getAttribute('data-tab');
            if (tabName) {
                switchTab(tabName);
            } else {
                // Fallback: Get from onclick attribute
                const onClick = this.getAttribute('onclick');
                if (onClick && onClick.includes("switchTab('")) {
                    const match = onClick.match(/switchTab\('([^']+)'\)/);
                    if (match && match[1]) {
                        switchTab(match[1]);
                    }
                }
            }
        });
    });
    
    // Initialize buttons
    document.getElementById('execute-query-btn').addEventListener('click', executeQuery);
    document.getElementById('clear-query-btn').addEventListener('click', clearQuery);
    document.getElementById('create-db-btn').addEventListener('click', createDatabase);
    document.getElementById('create-table-btn').addEventListener('click', createTable);
    document.getElementById('insert-row-btn').addEventListener('click', insertRow);

     // Add keyboard shortcuts
    document.addEventListener('keydown', function(event) {
        // Ctrl+Enter or Cmd+Enter to execute query
        if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
            event.preventDefault();
            executeQuery();
        }
        
        // Esc to clear query
        if (event.key === 'Escape' && document.activeElement.id === 'query-input') {
            clearQuery();
        }
    });
    
    // Close modals when clicking outside
    document.querySelectorAll('.modal').forEach(modal => {
        modal.addEventListener('click', function(e) {
            if (e.target === this) {
                this.classList.remove('active');
            }
        });
    });
    
    console.log("MyRDBMS Frontend Ready!");
});

// ==================== DATABASE FUNCTIONS ====================

// Load databases from server
async function loadDatabases() {
    try {
        const response = await fetch(`${API_BASE}/databases`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        renderDatabaseList(data.databases || []);
        setStatus('Databases loaded');
    } catch (error) {
        console.error('Failed to load databases:', error);
        showError('Failed to load databases. Make sure API server is running.');
    }
}

// Render database list
function renderDatabaseList(databases) {
    const container = document.getElementById('database-list');
    container.innerHTML = '';
    
    if (databases.length === 0) {
        container.innerHTML = '<div class="empty-state">No databases found</div>';
        return;
    }
    
    databases.forEach(db => {
        const dbElement = document.createElement('div');
        dbElement.className = 'database-item';
        dbElement.innerHTML = `
            <i class="fas fa-database"></i>
            <span>${db}</span>
        `;

        dbElement.addEventListener('click', (event) => {
            event.stopPropagation();
            selectDatabase(db, dbElement);
        });

        container.appendChild(dbElement);
    });
}

// Select a database
async function selectDatabase(dbName, dbElement) {
    if (activeDatabaseElement === dbElement) return;
    
    currentDatabase = dbName;
    
    // Update UI
    document.querySelectorAll('.database-item').forEach(el => {
        el.classList.remove('active');
    });
    dbElement.classList.add('active');
    activeDatabaseElement = dbElement;
    
    // Load tables for this database
    await loadTables(dbName);
    setStatus(`Selected database: ${dbName}`);
}

// Load tables for a database
async function loadTables(dbName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables`);
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        
        const data = await response.json();
        renderTableList(dbName, data.tables || []);
    } catch (error) {
        console.error(`Error loading tables for ${dbName}:`, error);
        showError(`Failed to load tables: ${error.message}`);
    }
}

// Render table list
function renderTableList(dbName, tables) {
    if (!activeDatabaseElement) return;

    let tablesList = activeDatabaseElement.querySelector('.tables-list');
    if (!tablesList) {
        tablesList = document.createElement('div');
        tablesList.className = 'tables-list';
        activeDatabaseElement.appendChild(tablesList);
    }

    tablesList.innerHTML = '';

    if (tables.length === 0) {
        tablesList.innerHTML = '<div class="empty-state">No tables found</div>';
        return;
    }

    tables.forEach(table => {
        const tableElement = document.createElement('div');
        tableElement.className = 'table-item';
        tableElement.innerHTML = `
            <i class="fas fa-table"></i>
            <span>${table}</span>
        `;

        tableElement.addEventListener('click', (event) => {
            event.stopPropagation();
            selectTable(dbName, table, tableElement);
        });

        tablesList.appendChild(tableElement);
    });
}

// Select a table
async function selectTable(dbName, tableName, tableElement) {
    currentTable = tableName;
    
    // Update UI
    document.querySelectorAll('.table-item').forEach(el => {
        el.classList.remove('active');
    });
    tableElement.classList.add('active');
    
    // Update table name in tabs
    document.getElementById('table-name').textContent = `Table: ${tableName}`;
    document.getElementById('schema-table-name').textContent = `Schema: ${tableName}`;
    
    // Load table data and schema
    await Promise.all([
        loadTableData(dbName, tableName),
        loadTableSchema(dbName, tableName)
    ]);
    
    // Switch to data tab
    switchTab('data');
    setStatus(`Selected table: ${tableName}`);
}

// ==================== TABLE DATA FUNCTIONS ====================

// Load table data
async function loadTableData(dbName, tableName) {
    try {
        console.log(`Loading data for ${dbName}.${tableName}`);
        
        // Try direct data endpoint first
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/data`);
        
        if (response.ok) {
            const data = await response.json();
            console.log("Data endpoint response:", data);
            renderTableData(data.rows || [], data.schema);
        } else {
            // Fallback to SELECT query
            await loadTableDataViaQuery(dbName, tableName);
        }
        
    } catch (error) {
        console.error(`Error loading table data:`, error);
        renderEmptyTable(`Failed to load data: ${error.message}`);
    }
}

// Load table data via SELECT query
async function loadTableDataViaQuery(dbName, tableName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/execute`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ 
                query: `SELECT * FROM ${tableName} LIMIT 100`
            })
        });
        
        const data = await response.json();
        console.log("SELECT query response:", data);
        
        if (!data.success) {
            renderEmptyTable(`Error: ${data.message || data.error}`);
        } else {
            const rows = data.data || [];
            renderTableData(rows, null);
        }
    } catch (error) {
        renderEmptyTable(`Query failed: ${error.message}`);
    }
}

// Render table data
function renderTableData(rows, schema) {
    const table = document.getElementById('data-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    
    // Clear existing data
    thead.innerHTML = '';
    tbody.innerHTML = '';
    
    if (rows.length === 0) {
        renderEmptyTable();
        return;
    }
    
    // Get column names
    let columns = [];
    if (schema && schema.columns) {
        columns = schema.columns.map(col => col.name);
    } else if (rows.length > 0) {
        columns = Object.keys(rows[0]);
    }
    
    // Create header
    const headerRow = thead.insertRow();
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        headerRow.appendChild(th);
    });
    
    // Add actions column header
    const actionsTh = document.createElement('th');
    actionsTh.textContent = 'Actions';
    actionsTh.style.width = '100px';
    headerRow.appendChild(actionsTh);
    
    // Create rows
    rows.forEach((rowData, rowIndex) => {
        const row = tbody.insertRow();
        
        // Add data cells
        columns.forEach(col => {
            const cell = row.insertCell();
            const value = rowData[col];
            cell.textContent = value !== null && value !== undefined ? value : 'NULL';
            cell.title = value !== null && value !== undefined ? String(value) : 'NULL';
        });
        
        // Add actions cell
        const actionsCell = row.insertCell();
        actionsCell.innerHTML = `
            <button class="btn btn-sm btn-outline-primary" onclick="editRow(${rowIndex})" title="Edit">
                <i class="fas fa-edit"></i>
            </button>
            <button class="btn btn-sm btn-outline-danger" onclick="deleteRow(${rowIndex})" title="Delete">
                <i class="fas fa-trash"></i>
            </button>
        `;
    });
}

// Render empty table
function renderEmptyTable(message = 'No data available') {
    const table = document.getElementById('data-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    
    thead.innerHTML = '';
    tbody.innerHTML = '';
    
    const row = tbody.insertRow();
    const cell = row.insertCell();
    cell.colSpan = 3;
    cell.textContent = message;
    cell.className = 'text-center text-muted py-4';
}

// ==================== SCHEMA FUNCTIONS ====================

// Load table schema
async function loadTableSchema(dbName, tableName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/schema`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }
        
        const schema = await response.json();
        renderTableSchema(schema);
    } catch (error) {
        console.error(`Error loading schema:`, error);
        renderEmptySchema(`Failed to load schema: ${error.message}`);
    }
}

// Render table schema
function renderTableSchema(schema) {
    const tbody = document.querySelector('#schema-table tbody');
    tbody.innerHTML = '';
    
    if (!schema || !schema.columns || schema.columns.length === 0) {
        renderEmptySchema('No schema information available');
        return;
    }
    
    schema.columns.forEach(col => {
        const row = tbody.insertRow();
        
        // Name
        const nameCell = row.insertCell();
        nameCell.textContent = col.name || 'unknown';
        
        // Type
        const typeCell = row.insertCell();
        typeCell.textContent = col.type || col.data_type || 'TEXT';
        
        // Constraints
        const constraintsCell = row.insertCell();
        if (col.constraints && col.constraints.length > 0) {
            constraintsCell.textContent = Array.isArray(col.constraints) 
                ? col.constraints.join(', ') 
                : String(col.constraints);
        } else {
            constraintsCell.textContent = '-';
        }
        
        // Nullable
        const nullableCell = row.insertCell();
        if (col.constraints && col.constraints.includes('NOT NULL')) {
            nullableCell.textContent = 'NO';
            nullableCell.className = 'text-danger';
        } else {
            nullableCell.textContent = 'YES';
            nullableCell.className = 'text-success';
        }
    });
}

// Render empty schema
function renderEmptySchema(message) {
    const tbody = document.querySelector('#schema-table tbody');
    tbody.innerHTML = '';
    
    const row = tbody.insertRow();
    const cell = row.insertCell();
    cell.colSpan = 4;
    cell.textContent = message;
    cell.className = 'text-center text-muted py-3';
}

// ==================== QUERY EXECUTION ====================

// Execute SQL query
// Execute SQL query - UPDATED VERSION
async function executeQuery() {
    if (!currentDatabase) {
        showError('Please select a database first');
        return;
    }
    
    let query = document.getElementById('query-input').value.trim();
    if (!query) {
        showError('Please enter a query');
        return;
    }
    
    // Remove trailing semicolon if present
    if (query.endsWith(';')) {
        query = query.slice(0, -1);
    }
    
    const startTime = performance.now();
    
    try {
        const response = await fetch(`${API_BASE}/databases/${currentDatabase}/execute`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query })
        });
        
        const data = await response.json();
        const endTime = performance.now();
        const executionTime = (endTime - startTime).toFixed(2);
        
        console.log("🎯 Query Execution Result:", data);
        
        // MyRDBMS specific handling
        displayQueryResults(data, executionTime);
        
        // Refresh if structure changed
        const upperQuery = query.toUpperCase();
        if (upperQuery.includes('CREATE') || 
            upperQuery.includes('DROP') || 
            upperQuery.includes('INSERT') || 
            upperQuery.includes('DELETE') ||
            upperQuery.includes('UPDATE') ||
            upperQuery.includes('ALTER')) {
            await refreshAfterQuery();
        }
        
    } catch (error) {
        console.error("Query execution error:", error);
        showError('Failed to execute query: ' + error.message);
    }
}

// Display query results
function displayQueryResults(data, executionTime) {
    const container = document.getElementById('results-container');
    container.innerHTML = '';
    
    console.log("🎯 Display Query Results - Raw Data:", data);
    
    // Determine query type from message
    const isUpdateQuery = data.message && data.message.includes('updated');
    const isDeleteQuery = data.message && data.message.includes('deleted');
    const isInsertQuery = data.message && data.message.includes('inserted');
    const isModificationQuery = isUpdateQuery || isDeleteQuery || isInsertQuery;
    
    // Choose alert color based on success
    const alertClass = data.success !== false ? 
        (isModificationQuery ? 'alert-success' : 'alert-info') : 
        'alert-danger';
    
    // ALWAYS show the message first
    let html = `<div class="alert ${alertClass}">`;
    html += `<strong>${data.success !== false ? 'Success' : 'Error'}:</strong> ${data.message || 'Query executed'}<br>`;
    html += `Execution time: ${executionTime}ms`;
    
    // Add row count if available (for UPDATE/DELETE/INSERT)
    if (data.row_count !== undefined || data.count !== undefined) {
        const count = data.row_count || data.count;
        if (count > 0) {
            html += `<br>Affected rows: ${count}`;
        }
    }
    html += `</div>`;
    
    // Check for data regardless of success flag
    const rows = data.data || [];
    
    console.log("Rows to display:", rows);
    console.log("Columns from backend:", data.columns);
    console.log("Is modification query?", isModificationQuery);
    
    // Only show table for SELECT queries with data
    if (rows.length > 0 && !isModificationQuery) {
        // Use columns from backend if available, otherwise infer
        const columns = data.columns || (rows.length > 0 ? Object.keys(rows[0]) : []);
        
        console.log("Using columns:", columns);
        
        html += `
            <div class="alert alert-success">
                ${data.row_count || rows.length} row(s) returned
            </div>
            <div class="table-responsive">
                <table class="table table-bordered table-hover query-results">
                    <thead>
                        <tr>
        `;
        
        // Header
        columns.forEach(col => {
            html += `<th>${col}</th>`;
        });
        html += '</tr></thead><tbody>';
        
        // Rows
        rows.forEach(row => {
            html += '<tr>';
            columns.forEach(col => {
                const value = row[col];
                // Format based on value type
                if (value === null || value === undefined || value === '') {
                    html += `<td><em class="text-muted">NULL</em></td>`;
                } else if (typeof value === 'boolean') {
                    html += `<td><span class="badge ${value ? 'bg-success' : 'bg-secondary'}">${value ? 'TRUE' : 'FALSE'}</span></td>`;
                } else if (typeof value === 'number') {
                    html += `<td class="text-end">${value}</td>`;
                } else {
                    html += `<td>${String(value)}</td>`;
                }
            });
            html += '</tr>';
        });
        
        html += '</tbody></table></div>';
    } else if (rows.length === 0 && !isModificationQuery && data.success !== false) {
        // Empty result set for SELECT query ONLY (not for UPDATE/DELETE/INSERT)
        html += `<div class="alert alert-warning">0 rows returned</div>`;
    }
    
    container.innerHTML = html;
    setStatus(`Query executed in ${executionTime}ms`);
    document.getElementById('execution-time').textContent = `${executionTime}ms`;
}

// MyRDBMS Response Helper
// MyRDBMS Response Helper - UPDATED
function handleMyRDBMSResponse(data) {
    console.log("🎯 Handling MyRDBMS Response:", data);
    
    // Check if this is an UPDATE/DELETE/INSERT response (has count but no data)
    const isUpdateResponse = (
        data.count !== undefined || 
        (data.message && data.message.includes('row') && data.message.includes('updated')) ||
        (data.message && data.message.includes('row') && data.message.includes('deleted')) ||
        (data.message && data.message.includes('row') && data.message.includes('inserted'))
    );
    
    // Check if this is a SELECT response (has data array)
    const isSelectResponse = Array.isArray(data.data) && data.columns;
    
    // Determine success
    const isError = data.error || 
                   (data.message && data.message.toLowerCase().includes('error'));
    
    // Build normalized response
    const normalized = {
        success: !isError,
        message: data.message || (data.error ? 'Error' : 'Query executed'),
        data: isSelectResponse ? data.data : (isUpdateResponse ? [] : data.data || []),
        columns: data.columns || [],
        row_count: data.row_count || data.count || (data.data ? data.data.length : 0),
        error: data.error,
        // Preserve original fields for debugging
        _original: data
    };
    
    console.log("🎯 Normalized to:", normalized);
    return normalized;
}

// Updated executeQuery using helper
async function executeQuery() {
    if (!currentDatabase) {
        showError('Please select a database first');
        return;
    }
    
    let query = document.getElementById('query-input').value.trim();
    if (!query) {
        showError('Please enter a query');
        return;
    }
    
    // Remove trailing semicolon if present
    if (query.endsWith(';')) {
        query = query.slice(0, -1);
    }
    
    const startTime = performance.now();
    
    try {
        const response = await fetch(`${API_BASE}/databases/${currentDatabase}/execute`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query })
        });
        
        const rawData = await response.json();
        const endTime = performance.now();
        const executionTime = (endTime - startTime).toFixed(2);
        
        console.log("🎯 Raw MyRDBMS Response:", rawData);
        
        // Normalize the response
        const normalizedData = handleMyRDBMSResponse(rawData);
        
        console.log("🎯 Normalized Data:", normalizedData);
        
        displayQueryResults(normalizedData, executionTime);
        
        // Refresh if structure changed
        const upperQuery = query.toUpperCase();
        if (upperQuery.includes('CREATE') || 
            upperQuery.includes('DROP') || 
            upperQuery.includes('INSERT') || 
            upperQuery.includes('DELETE') ||
            upperQuery.includes('UPDATE') ||
            upperQuery.includes('ALTER')) {
            await refreshAfterQuery();
        }
        
    } catch (error) {
        console.error("Query execution error:", error);
        showError('Failed to execute query: ' + error.message);
    }
}

// ==================== CRUD OPERATIONS ====================

// Insert a new row
async function insertRow() {
    if (!currentDatabase || !currentTable) {
        showError('Please select a database and table first');
        return;
    }
    
    try {
        // Get column names
        const columns = await getTableColumns(currentDatabase, currentTable);
        if (!columns || columns.length === 0) {
            showError('Could not get table columns. Table might be empty or not exist.');
            return;
        }
        
        // Build INSERT query with sample values
        const values = columns.map(col => {
            const value = prompt(`Enter value for "${col}":`, '');
            if (value === null) throw new Error('User cancelled');
            
            // Handle different value types
            if (value === '' || value.toLowerCase() === 'null') {
                return 'NULL';
            } else if (!isNaN(value) && value.trim() !== '') {
                return value; // Number
            } else if (value.toLowerCase() === 'true' || value.toLowerCase() === 'false') {
                return value.toUpperCase(); // Boolean
            } else {
                return `'${value.replace(/'/g, "''")}'`; // String
            }
        });
        
        const query = `INSERT INTO ${currentTable} VALUES (${values.join(', ')})`;
        
        // Execute INSERT
        const response = await fetch(`${API_BASE}/databases/${currentDatabase}/execute`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ query })
        });
        
        const result = await response.json();
        
        if (result.error) {
            showError(`Insert failed: ${result.error}`);
        } else {
            showSuccess('Row inserted successfully');
            // Refresh table data
            await loadTableData(currentDatabase, currentTable);
        }
        
    } catch (error) {
        if (error.message !== 'User cancelled') {
            showError(`Insert error: ${error.message}`);
        }
    }
}

// Refresh databases/tables after query
async function refreshAfterQuery() {
    try {
        console.log("Refreshing database list...");
        await loadDatabases();
        
        if (currentDatabase) {
            console.log(`Refreshing tables for ${currentDatabase}...`);
            await loadTables(currentDatabase);
            
            // If we have a current table, refresh its data too
            if (currentTable) {
                console.log(`Refreshing data for ${currentTable}...`);
                await loadTableData(currentDatabase, currentTable);
            }
        }
    } catch (error) {
        console.error("Error refreshing:", error);
    }
}

// Get table columns
async function getTableColumns(dbName, tableName) {
    try {
        // Try schema endpoint first
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/schema`);
        
        if (response.ok) {
            const data = await response.json();
            if (data.columns) {
                return data.columns.map(col => col.name || col.column_name);
            }
        }
        
        // Fallback: Execute SELECT to get column names
        const queryResponse = await fetch(`${API_BASE}/databases/${dbName}/execute`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ 
                query: `SELECT * FROM ${tableName} LIMIT 1`
            })
        });
        
        const queryData = await queryResponse.json();
        if (queryData.columns) {
            return queryData.columns;
        }
        
        return [];
        
    } catch (error) {
        console.error('Error getting columns:', error);
        return [];
    }
}

// Edit a row (placeholder)
function editRow(rowIndex) {
    showInfo('Edit functionality coming soon!');
}

// Delete a row (placeholder)
async function deleteRow(rowIndex) {
    if (!confirm('Are you sure you want to delete this row?')) return;
    
    showInfo('Delete functionality requires primary key information. Coming soon!');
}

// ==================== TAB MANAGEMENT ====================

// Switch tabs
function switchTab(tabName) {
    // Update tab buttons
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
        if (tab.getAttribute('data-tab') === tabName) {
            tab.classList.add('active');
        }
    });
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
        if (content.id === `${tabName}-tab`) {
            content.classList.add('active');
        }
    });
    
    // Load data if needed
    if (tabName === 'data' && currentDatabase && currentTable) {
        loadTableData(currentDatabase, currentTable);
    } else if (tabName === 'schema' && currentDatabase && currentTable) {
        loadTableSchema(currentDatabase, currentTable);
    }
}

// ==================== DATABASE/TABLE CREATION ====================

// Create database
function createDatabase() {
    const dbName = prompt('Enter database name:');
    if (!dbName) return;
    
    fetch(`${API_BASE}/databases`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ name: dbName })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
        } else {
            showSuccess(`Database "${dbName}" created`);
            loadDatabases();
        }
    })
    .catch(error => {
        showError('Failed to create database');
        console.error(error);
    });
}

// Create table
function createTable() {
    if (!currentDatabase) {
        showError('Please select a database first');
        return;
    }
    
    const tableName = prompt('Enter table name:');
    if (!tableName) return;
    
    const columnsInput = prompt('Enter columns (e.g., "id INT PRIMARY KEY, name VARCHAR(50), age INT"):');
    if (!columnsInput) return;
    
    const query = `CREATE TABLE ${tableName} (${columnsInput})`;
    
    // Execute in query editor
    document.getElementById('query-input').value = query;
    executeQuery();
}

// ==================== UTILITY FUNCTIONS ====================

// Set status message
function setStatus(message) {
    const statusEl = document.getElementById('status-message');
    if (statusEl) {
        statusEl.textContent = message;
    }
}

// Show success message
function showSuccess(message) {
    setStatus(`✅ ${message}`);
    showToast(message, 'success');
}

// Show error message
function showError(message) {
    setStatus(`❌ ${message}`);
    showToast(message, 'error');
}

// Show info message
function showInfo(message) {
    setStatus(`ℹ️ ${message}`);
    showToast(message, 'info');
}

// Show toast notification
function showToast(message, type = 'info') {
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    toast.style.cssText = `
        position: fixed;
        top: 20px;
        right: 20px;
        padding: 12px 20px;
        border-radius: 4px;
        color: white;
        z-index: 1000;
        animation: fadeIn 0.3s;
    `;
    
    if (type === 'success') {
        toast.style.backgroundColor = '#28a745';
    } else if (type === 'error') {
        toast.style.backgroundColor = '#dc3545';
    } else {
        toast.style.backgroundColor = '#17a2b8';
    }
    
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.style.animation = 'fadeOut 0.3s';
        setTimeout(() => {
            if (toast.parentNode) {
                toast.parentNode.removeChild(toast);
            }
        }, 300);
    }, 3000);
}

// Clear query editor
function clearQuery() {
    document.getElementById('query-input').value = '';
}

// ==================== KEYBOARD SHORTCUTS ====================

// Add keyboard shortcuts
document.addEventListener('keydown', function(event) {
    // Ctrl+Enter or Cmd+Enter to execute query
    if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
        event.preventDefault();
        executeQuery();
    }
    
    // Esc to clear query
    if (event.key === 'Escape' && document.activeElement.id === 'query-input') {
        clearQuery();
    }
});

// ==================== MODAL FUNCTIONS ====================

function createDatabase() {
    openModal('create-database-modal');
}

function confirmCreateDatabase() {
    const dbName = document.getElementById('new-db-name').value.trim();
    if (!dbName) {
        showError('Please enter a database name');
        return;
    }
    
    fetch(`${API_BASE}/databases`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ name: dbName })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
        } else {
            showSuccess(`Database "${dbName}" created`);
            closeModal('create-database-modal');
            loadDatabases();
            document.getElementById('new-db-name').value = '';
        }
    })
    .catch(error => {
        showError('Failed to create database: ' + error.message);
    });
}

function createTable() {
    if (!currentDatabase) {
        showError('Please select a database first');
        return;
    }
    openModal('create-table-modal');
}

function addColumn() {
    const container = document.getElementById('columns-container');
    const columnDiv = document.createElement('div');
    columnDiv.className = 'column-definition';
    columnDiv.innerHTML = `
        <input type="text" placeholder="Column name" class="col-name">
        <select class="col-type">
            <option value="INT">INT</option>
            <option value="VARCHAR(50)">VARCHAR(50)</option>
            <option value="TEXT">TEXT</option>
            <option value="BOOLEAN">BOOLEAN</option>
            <option value="DECIMAL">DECIMAL</option>
            <option value="DATE">DATE</option>
        </select>
        <label><input type="checkbox" class="col-pk"> PK</label>
        <label><input type="checkbox" class="col-unique"> Unique</label>
        <label><input type="checkbox" class="col-nullable"> Nullable</label>
        <button class="btn btn-danger btn-small" onclick="removeColumn(this)">×</button>
    `;
    container.appendChild(columnDiv);
}

function removeColumn(button) {
    const columnDiv = button.closest('.column-definition');
    if (columnDiv && document.querySelectorAll('.column-definition').length > 1) {
        columnDiv.remove();
    }
}

function confirmCreateTable() {
    const tableName = document.getElementById('new-table-name').value.trim();
    if (!tableName) {
        showError('Please enter a table name');
        return;
    }
    
    const columns = [];
    document.querySelectorAll('.column-definition').forEach(colDiv => {
        const name = colDiv.querySelector('.col-name').value.trim();
        const type = colDiv.querySelector('.col-type').value;
        const isPK = colDiv.querySelector('.col-pk').checked;
        const isUnique = colDiv.querySelector('.col-unique').checked;
        const isNullable = colDiv.querySelector('.col-nullable').checked;
        
        if (name) {
            let columnDef = `${name} ${type}`;
            if (isPK) columnDef += ' PRIMARY KEY';
            if (isUnique) columnDef += ' UNIQUE';
            if (!isNullable) columnDef += ' NOT NULL';
            columns.push(columnDef);
        }
    });
    
    if (columns.length === 0) {
        showError('Please define at least one column');
        return;
    }
    
    const query = `CREATE TABLE ${tableName} (\n    ${columns.join(',\n    ')}\n)`;
    
    // Execute query
    fetch(`${API_BASE}/databases/${currentDatabase}/execute`, {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ query })
    })
    .then(response => response.json())
    .then(data => {
        if (data.error) {
            showError(data.error);
        } else {
            showSuccess(`Table "${tableName}" created`);
            closeModal('create-table-modal');
            resetCreateTableForm();
            loadTables(currentDatabase);
        }
    })
    .catch(error => {
        showError('Failed to create table: ' + error.message);
    });
}

function resetCreateTableForm() {
    document.getElementById('new-table-name').value = '';
    const container = document.getElementById('columns-container');
    container.innerHTML = `
        <div class="column-definition">
            <input type="text" placeholder="Column name" class="col-name">
            <select class="col-type">
                <option value="INT">INT</option>
                <option value="VARCHAR(50)">VARCHAR(50)</option>
                <option value="TEXT">TEXT</option>
                <option value="BOOLEAN">BOOLEAN</option>
                <option value="DECIMAL">DECIMAL</option>
            </select>
            <label><input type="checkbox" class="col-pk"> PK</label>
            <label><input type="checkbox" class="col-unique"> Unique</label>
            <label><input type="checkbox" class="col-nullable"> Nullable</label>
            <button class="btn btn-danger btn-small" onclick="removeColumn(this)">×</button>
        </div>
    `;
}

function openModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// Close modals when clicking outside
document.querySelectorAll('.modal').forEach(modal => {
    modal.addEventListener('click', function(e) {
        if (e.target === this) {
            this.classList.remove('active');
        }
    });
});

// ==================== CSS FOR TOASTS ====================

// Add CSS for toast animations
const style = document.createElement('style');
style.textContent = `
    @keyframes fadeIn {
        from { opacity: 0; transform: translateY(-20px); }
        to { opacity: 1; transform: translateY(0); }
    }
    
    @keyframes fadeOut {
        from { opacity: 1; transform: translateY(0); }
        to { opacity: 0; transform: translateY(-20px); }
    }
    
    .empty-state {
        padding: 10px;
        color: #6c757d;
        font-style: italic;
        text-align: center;
    }
`;
document.head.appendChild(style);

// Test if buttons work
console.log('executeQuery function:', typeof executeQuery);
console.log('clearQuery function:', typeof clearQuery);
console.log('createDatabase function:', typeof createDatabase);
console.log('createTable function:', typeof createTable);
console.log('insertRow function:', typeof insertRow);

// Test if tabs work
console.log('Tabs found:', document.querySelectorAll('.tab').length);
console.log('Active tab:', document.querySelector('.tab.active')?.getAttribute('data-tab'));

// Test API connection
fetch('http://localhost:5000/api/health')
    .then(r => r.json())
    .then(data => console.log('API Health:', data));

async function debugDatabase() {
    console.clear();
    console.log("=== Database Debug ===");
    
    const API_BASE = 'http://localhost:5000/api';
    
    // 1. List all databases
    console.log("1. Listing databases...");
    const dbsResponse = await fetch(`${API_BASE}/databases`);
    const dbsData = await dbsResponse.json();
    console.log("Databases:", dbsData.databases);
    
    // 2. For each database, list tables
    for (const dbName of dbsData.databases) {
        console.log(`\n2. Database: ${dbName}`);
        
        const tablesResponse = await fetch(`${API_BASE}/databases/${dbName}/tables`);
        const tablesData = await tablesResponse.json();
        console.log(`   Tables:`, tablesData.tables);
        
        // 3. For each table, count rows
        for (const tableName of tablesData.tables) {
            console.log(`\n3. Table: ${tableName}`);
            
            // Get schema
            const schemaResponse = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/schema`);
            const schemaData = await schemaResponse.json();
            console.log(`   Schema:`, schemaData.schema?.columns?.map(c => c.name) || 'No schema');
            
            // Count rows
            const countQuery = `SELECT COUNT(*) as count FROM ${tableName}`;
            const countResponse = await fetch(`${API_BASE}/databases/${dbName}/execute`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ query: countQuery })
            });
            const countData = await countResponse.json();
            console.log(`   Row count:`, countData.data?.[0]?.count || 0);
            
            // Get sample data
            const sampleResponse = await fetch(`${API_BASE}/databases/${dbName}/execute`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ query: `SELECT * FROM ${tableName} LIMIT 5` })
            });
            const sampleData = await sampleResponse.json();
            console.log(`   Sample data (first 5 rows):`, sampleData.data);
        }
    }
    
    console.log("\n=== Debug Complete ===");
}