// Global state
let currentDatabase = null;
let currentTable = null;
const API_BASE = 'http://localhost:5000/api';

// Initialize the application
document.addEventListener('DOMContentLoaded', function() {
    loadDatabases();
});

// Load databases from server
async function loadDatabases() {
    try {
        const response = await fetch(`${API_BASE}/databases`);
        const data = await response.json();
        renderDatabaseList(data.databases);
        setStatus('Databases loaded');
    } catch (error) {
        showError('Failed to load databases');
        console.error(error);
    }
}

// Render database list
function renderDatabaseList(databases) {
    const container = document.getElementById('database-list');
    container.innerHTML = '';
    
    databases.forEach(db => {
        const dbElement = document.createElement('div');
        dbElement.className = 'database-item';
        dbElement.innerHTML = `
            <i class="fas fa-database"></i>
            <span>${db}</span>
        `;
        
        dbElement.onclick = () => selectDatabase(db);
        container.appendChild(dbElement);
    });
}

// Select a database
async function selectDatabase(dbName) {
    currentDatabase = dbName;
    
    // Update UI
    document.querySelectorAll('.database-item').forEach(el => {
        el.classList.remove('active');
    });
    event.currentTarget.classList.add('active');
    
    // Load tables for this database
    await loadTables(dbName);
    setStatus(`Selected database: ${dbName}`);
}

// Load tables for a database
async function loadTables(dbName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables`);
        const data = await response.json();
        renderTableList(dbName, data.tables);
    } catch (error) {
        console.error(error);
    }
}

// Render table list
function renderTableList(dbName, tables) {
    // Find or create tables list for this database
    let dbItem = event.currentTarget;
    let tablesList = dbItem.querySelector('.tables-list');
    
    if (!tablesList) {
        tablesList = document.createElement('div');
        tablesList.className = 'tables-list';
        dbItem.appendChild(tablesList);
    }
    
    tablesList.innerHTML = '';
    
    tables.forEach(table => {
        const tableElement = document.createElement('div');
        tableElement.className = 'table-item';
        tableElement.innerHTML = `
            <i class="fas fa-table"></i>
            <span>${table}</span>
        `;
        
        tableElement.onclick = (e) => {
            e.stopPropagation();
            selectTable(dbName, table);
        };
        
        tablesList.appendChild(tableElement);
    });
}

// Select a table
async function selectTable(dbName, tableName) {
    currentTable = tableName;
    
    // Update UI
    document.querySelectorAll('.table-item').forEach(el => {
        el.classList.remove('active');
    });
    event.currentTarget.classList.add('active');
    
    // Update table name in tabs
    document.getElementById('table-name').textContent = `Table: ${tableName}`;
    document.getElementById('schema-table-name').textContent = `Schema: ${tableName}`;
    
    // Switch to data tab and load data
    switchTab('data');
    await loadTableData(dbName, tableName);
    await loadTableSchema(dbName, tableName);
    
    setStatus(`Selected table: ${tableName}`);
}

// Load table data
async function loadTableData(dbName, tableName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/data`);
        const data = await response.json();
        renderTableData(data.rows);
    } catch (error) {
        console.error(error);
    }
}

// Render table data
function renderTableData(rows) {
    const table = document.getElementById('data-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    
    // Clear existing data
    thead.innerHTML = '';
    tbody.innerHTML = '';
    
    if (rows.length === 0) {
        const row = tbody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = 1;
        cell.textContent = 'No data found';
        cell.className = 'placeholder';
        return;
    }
    
    // Create header
    const headerRow = thead.insertRow();
    Object.keys(rows[0]).forEach(key => {
        const th = document.createElement('th');
        th.textContent = key;
        headerRow.appendChild(th);
    });
    
    // Create rows
    rows.forEach(rowData => {
        const row = tbody.insertRow();
        Object.values(rowData).forEach(value => {
            const cell = row.insertCell();
            cell.textContent = value;
        });
    });
}

// Load table schema
async function loadTableSchema(dbName, tableName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/schema`);
        const schema = await response.json();
        renderTableSchema(schema);
    } catch (error) {
        console.error(error);
    }
}

// Render table schema
function renderTableSchema(schema) {
    const tbody = document.querySelector('#schema-table tbody');
    tbody.innerHTML = '';
    
    if (!schema.columns || schema.columns.length === 0) {
        const row = tbody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = 4;
        cell.textContent = 'No schema found';
        cell.className = 'placeholder';
        return;
    }
    
    schema.columns.forEach(col => {
        const row = tbody.insertRow();
        
        const nameCell = row.insertCell();
        nameCell.textContent = col.name;
        
        const typeCell = row.insertCell();
        typeCell.textContent = col.type;
        
        const constraintsCell = row.insertCell();
        constraintsCell.textContent = col.constraints ? col.constraints.join(', ') : '';
        
        const nullableCell = row.insertCell();
        nullableCell.textContent = col.constraints && col.constraints.includes('NOT NULL') ? 'NO' : 'YES';
    });
}

// Execute SQL query
async function executeQuery() {
    if (!currentDatabase) {
        showError('Please select a database first');
        return;
    }
    
    const query = document.getElementById('query-input').value.trim();
    if (!query) {
        showError('Please enter a query');
        return;
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
        
        displayQueryResults(data, executionTime);
        
        // Refresh databases/tables if query modified structure
        if (query.toUpperCase().includes('CREATE') || query.toUpperCase().includes('DROP')) {
            await loadDatabases();
        }
        
    } catch (error) {
        showError('Failed to execute query');
        console.error(error);
    }
}

// Display query results
function displayQueryResults(data, executionTime) {
    const container = document.getElementById('results-container');
    
    if (data.error) {
        container.innerHTML = `
            <div class="error-message">
                <strong>Error:</strong> ${data.error}
            </div>
        `;
        setStatus(`Error: ${data.error}`);
        return;
    }
    
    if (data.message) {
        container.innerHTML = `
            <div class="success-message">
                <strong>Success:</strong> ${data.message}
            </div>
        `;
        setStatus(data.message);
    } else if (data.rows) {
        if (data.rows.length === 0) {
            container.innerHTML = '<p class="placeholder">No rows returned</p>';
        } else {
            let html = `<p>${data.count} row(s) returned in ${executionTime}ms</p>`;
            html += '<table class="query-results">';
            
            // Header
            html += '<tr>';
            data.columns.forEach(col => {
                html += `<th>${col}</th>`;
            });
            html += '</tr>';
            
            // Rows
            data.rows.forEach(row => {
                html += '<tr>';
                data.columns.forEach(col => {
                    html += `<td>${row[col] || ''}</td>`;
                });
                html += '</tr>';
            });
            
            html += '</table>';
            container.innerHTML = html;
        }
        setStatus(`Query executed successfully in ${executionTime}ms`);
    }
    
    document.getElementById('execution-time').textContent = `${executionTime}ms`;
}

// Tab switching
function switchTab(tabName) {
    // Hide all tabs
    document.querySelectorAll('.tab-content').forEach(tab => {
        tab.classList.remove('active');
    });
    
    document.querySelectorAll('.tab').forEach(tab => {
        tab.classList.remove('active');
    });
    
    // Show selected tab
    document.getElementById(`${tabName}-tab`).classList.add('active');
    event.currentTarget.classList.add('active');
}

// Database creation
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
            closeModal('create-database-modal');
            document.getElementById('new-db-name').value = '';
            loadDatabases();
            setStatus(data.message);
        }
    })
    .catch(error => {
        showError('Failed to create database');
        console.error(error);
    });
}

// Table creation
function createTable() {
    openModal('create-table-modal');
}

function addColumn() {
    const container = document.getElementById('columns-container');
    const newColumn = document.querySelector('.column-definition').cloneNode(true);
    newColumn.querySelectorAll('input').forEach(input => {
        if (input.type !== 'checkbox') {
            input.value = '';
        } else {
            input.checked = false;
        }
    });
    container.appendChild(newColumn);
}

function removeColumn(button) {
    const container = document.getElementById('columns-container');
    if (container.children.length > 1) {
        button.closest('.column-definition').remove();
    }
}

function confirmCreateTable() {
    if (!currentDatabase) {
        showError('Please select a database first');
        return;
    }
    
    const tableName = document.getElementById('new-table-name').value.trim();
    if (!tableName) {
        showError('Please enter a table name');
        return;
    }
    
    // Collect column definitions
    const columns = [];
    document.querySelectorAll('.column-definition').forEach(colDiv => {
        const name = colDiv.querySelector('.col-name').value.trim();
        const type = colDiv.querySelector('.col-type').value;
        const constraints = [];
        
        if (colDiv.querySelector('.col-pk').checked) {
            constraints.push('PRIMARY KEY');
        }
        if (colDiv.querySelector('.col-unique').checked) {
            constraints.push('UNIQUE');
        }
        if (!colDiv.querySelector('.col-nullable').checked) {
            constraints.push('NOT NULL');
        }
        
        if (name) {
            columns.push({
                name,
                type,
                constraints
            });
        }
    });
    
    if (columns.length === 0) {
        showError('Please add at least one column');
        return;
    }
    
    // Build CREATE TABLE query
    let query = `CREATE TABLE ${tableName} (`;
    query += columns.map(col => {
        let def = `${col.name} ${col.type}`;
        if (col.constraints.length > 0) {
            def += ' ' + col.constraints.join(' ');
        }
        return def;
    }).join(', ');
    query += ')';
    
    // Execute the query
    document.getElementById('query-input').value = query;
    executeQuery();
    closeModal('create-table-modal');
    
    // Reset form
    document.getElementById('new-table-name').value = '';
    const columnsContainer = document.getElementById('columns-container');
    columnsContainer.innerHTML = '<div class="column-definition">...</div>';
    addColumn();
}

// Modal functions
function openModal(modalId) {
    document.getElementById(modalId).classList.add('active');
}

function closeModal(modalId) {
    document.getElementById(modalId).classList.remove('active');
}

// Utility functions
function setStatus(message) {
    document.getElementById('status-message').textContent = message;
}

function showError(message) {
    setStatus(`Error: ${message}`);
    // You could add a toast notification here
}

function clearQuery() {
    document.getElementById('query-input').value = '';
}

function insertRow() {
    alert('Insert row functionality would be implemented here');
    // In a full implementation, you'd show a form to enter row data
}

// Close modals when clicking outside
document.querySelectorAll('.modal').forEach(modal => {
    modal.addEventListener('click', function(e) {
        if (e.target === this) {
            this.classList.remove('active');
        }
    });
});