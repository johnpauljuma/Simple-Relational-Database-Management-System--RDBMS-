// Global state
let currentDatabase = null;
let currentTable = null;
let activeDatabaseElement = null;
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

        dbElement.addEventListener('click', async (event) => {
            await selectDatabase(event, db);
        });

        container.appendChild(dbElement);
    });
}

// Select a database
async function selectDatabase(event, dbName) {
    currentDatabase = dbName;
    activeDatabaseElement = event.currentTarget;
    
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
    if (!activeDatabaseElement) return;

    let tablesList = activeDatabaseElement.querySelector('.tables-list');

    if (!tablesList) {
        tablesList = document.createElement('div');
        tablesList.className = 'tables-list';
        activeDatabaseElement.appendChild(tablesList);
    }

    tablesList.innerHTML = '';

    tables.forEach(table => {
        const tableElement = document.createElement('div');
        tableElement.className = 'table-item';
        tableElement.innerHTML = `
            <i class="fas fa-table"></i>
            <span>${table}</span>
        `;

        tableElement.addEventListener('click', async (event) => {
            event.stopPropagation();
            await selectTable(dbName, table);
        });

        tablesList.appendChild(tableElement);
    });
}

// Select a table
async function selectTable(dbName, tableName) {
    currentTable = tableName;
    
    // Update UI - highlight selected table
    document.querySelectorAll('.table-item').forEach(el => {
        el.classList.remove('active');
    });
    
    // Find and activate the clicked table
    const tableItems = document.querySelectorAll('.table-item');
    for (const item of tableItems) {
        const span = item.querySelector('span');
        if (span && span.textContent === tableName) {
            item.classList.add('active');
            break;
        }
    }
    
    // Update table name in tabs
    document.getElementById('table-name').textContent = `Table: ${tableName}`;
    document.getElementById('schema-table-name').textContent = `Schema: ${tableName}`;
    
    // Load table data
    await loadTableData(dbName, tableName);
    
    // Also load schema (optional)
    await loadTableSchema(dbName, tableName);
    
    // Switch to data tab
    switchTab('data');
    
    setStatus(`Selected table: ${tableName}`);
}

// Load table data
async function loadTableData(dbName, tableName) {
    try {
        console.log(`Loading data for ${dbName}.${tableName}`);
        
        // First, try to get data via API if you have an endpoint
        // If not, execute a SELECT query
        const response = await fetch(`${API_BASE}/databases/${dbName}/execute`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({ 
                query: `SELECT * FROM ${tableName}`
            })
        });
        
        const data = await response.json();
        
        if (data.error) {
            showError(`Error loading table data: ${data.error}`);
            renderEmptyTable();
        } else {
            renderTableData(data);
        }
        
    } catch (error) {
        console.error(`Error loading table data:`, error);
        showError(`Failed to load table data: ${error.message}`);
        
        // Fallback: Try alternative endpoint
        try {
            const altResponse = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/data`);
            const altData = await altResponse.json();
            renderTableData(altData);
        } catch (fallbackError) {
            renderEmptyTable();
        }
    }
}

// Render table data
function renderTableData(data) {
    const table = document.getElementById('data-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    
    // Clear existing data
    thead.innerHTML = '';
    tbody.innerHTML = '';
    
    // Check if we have data
    let rows = data.rows || [];
    let columns = data.columns || [];
    
    if (rows.length === 0) {
        // No data - show empty message
        const row = tbody.insertRow();
        const cell = row.insertCell();
        cell.colSpan = 1;
        cell.textContent = 'No data found in table';
        cell.className = 'placeholder';
        cell.style.textAlign = 'center';
        cell.style.padding = '2rem';
        return;
    }
    
    // Create header from columns or first row keys
    if (columns.length === 0 && rows.length > 0) {
        columns = Object.keys(rows[0]);
    }
    
    const headerRow = thead.insertRow();
    columns.forEach(col => {
        const th = document.createElement('th');
        th.textContent = col;
        headerRow.appendChild(th);
    });
    
    // Add actions column header
    const actionsTh = document.createElement('th');
    actionsTh.textContent = 'Actions';
    headerRow.appendChild(actionsTh);
    
    // Create rows
    rows.forEach((rowData, rowIndex) => {
        const row = tbody.insertRow();
        
        // Add data cells
        columns.forEach(col => {
            const cell = row.insertCell();
            const value = rowData[col];
            cell.textContent = value !== null && value !== undefined ? value : 'NULL';
            cell.title = `Type: ${typeof value}`;
        });
        
        // Add actions cell
        const actionsCell = row.insertCell();
        actionsCell.innerHTML = `
            <button class="btn btn-small" onclick="editRow(${rowIndex})">
                <i class="fas fa-edit"></i>
            </button>
            <button class="btn btn-small btn-danger" onclick="deleteRow(${rowIndex})">
                <i class="fas fa-trash"></i>
            </button>
        `;
        actionsCell.style.whiteSpace = 'nowrap';
    });
    
    // Update count display
    const countElement = document.querySelector('.row-count');
    if (countElement) {
        countElement.textContent = `${rows.length} row(s)`;
    }
}

// Render empty table state
function renderEmptyTable() {
    const table = document.getElementById('data-table');
    const thead = table.querySelector('thead');
    const tbody = table.querySelector('tbody');
    
    thead.innerHTML = '';
    tbody.innerHTML = '';
    
    const row = tbody.insertRow();
    const cell = row.insertCell();
    cell.colSpan = 1;
    cell.textContent = 'No data available. Try inserting some rows.';
    cell.className = 'placeholder';
    cell.style.textAlign = 'center';
    cell.style.padding = '2rem';
}

// Load table schema
async function loadTableSchema(dbName, tableName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/schema`);
        const schema = await response.json();
        renderTableSchema(schema);
    } catch (error) {
        console.error(`Error loading schema:`, error);
        // Schema tab will just be empty
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
        cell.textContent = 'No schema information available';
        cell.className = 'placeholder';
        return;
    }
    
    schema.columns.forEach(col => {
        const row = tbody.insertRow();
        
        const nameCell = row.insertCell();
        nameCell.textContent = col.name;
        
        const typeCell = row.insertCell();
        typeCell.textContent = col.type || 'TEXT';
        
        const constraintsCell = row.insertCell();
        constraintsCell.textContent = col.constraints ? col.constraints.join(', ') : '';
        
        const nullableCell = row.insertCell();
        const hasNotNull = col.constraints && col.constraints.some(c => 
            c.toUpperCase().includes('NOT NULL')
        );
        nullableCell.textContent = hasNotNull ? 'NO' : 'YES';
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

    // Show selected tab content
    const tabContent = document.getElementById(`${tabName}-tab`);
    if (tabContent) {
        tabContent.classList.add('active');
    }

    // Activate the corresponding tab button
    const tabButton = document.querySelector(`.tab[data-tab="${tabName}"]`);
    if (tabButton) {
        tabButton.classList.add('active');
    }

    // Optional refresh logic
    if (tabName === 'data' && currentDatabase && currentTable) {
        loadTableData(currentDatabase, currentTable);
    }

    if (tabName === 'schema' && currentDatabase && currentTable) {
        loadTableSchema(currentDatabase, currentTable);
    }
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

// Insert a new row
async function insertRow() {
    if (!currentDatabase || !currentTable) {
        showError('Please select a table first');
        return;
    }
    
    // Simple prompt for now - you can make this a modal later
    const columnNames = await getTableColumns(currentDatabase, currentTable);
    if (!columnNames || columnNames.length === 0) {
        showError('Could not get table columns');
        return;
    }
    
    let values = [];
    for (const col of columnNames) {
        const value = prompt(`Enter value for ${col}:`);
        if (value === null) return; // User cancelled
        values.push(value);
    }
    
    // Build INSERT query
    const valuesStr = values.map(v => 
        typeof v === 'string' ? `'${v.replace(/'/g, "''")}'` : v
    ).join(', ');
    
    const query = `INSERT INTO ${currentTable} VALUES (${valuesStr})`;
    
    try {
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
        showError(`Insert error: ${error.message}`);
    }
}

// Helper to get table columns
async function getTableColumns(dbName, tableName) {
    try {
        const response = await fetch(`${API_BASE}/databases/${dbName}/tables/${tableName}/schema`);
        const schema = await response.json();
        return schema.columns ? schema.columns.map(col => col.name) : [];
    } catch (error) {
        console.error('Error getting columns:', error);
        return [];
    }
}

// Edit a row
function editRow(rowIndex) {
    showError('Edit functionality not implemented yet');
    // You would implement a modal for editing
}

// Delete a row
async function deleteRow(rowIndex) {
    if (!currentDatabase || !currentTable) {
        showError('Please select a table first');
        return;
    }
    
    if (!confirm('Are you sure you want to delete this row?')) {
        return;
    }
    
    // Note: This is a simple implementation
    // In a real system, you'd need to know the primary key
    showError('Delete functionality needs primary key information');
}

// Close modals when clicking outside
document.querySelectorAll('.modal').forEach(modal => {
    modal.addEventListener('click', function(e) {
        if (e.target === this) {
            this.classList.remove('active');
        }
    });
});

// Utility functions for status messages
function showSuccess(message) {
    setStatus(`✅ ${message}`);
    // Optional: Add a toast notification
    const toast = document.createElement('div');
    toast.className = 'toast success';
    toast.textContent = message;
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.remove();
    }, 3000);
}

function showError(message) {
    setStatus(`❌ ${message}`);
    // Optional: Add a toast notification
    const toast = document.createElement('div');
    toast.className = 'toast error';
    toast.textContent = message;
    document.body.appendChild(toast);
    
    setTimeout(() => {
        toast.remove();
    }, 3000);
}