// ========================================
// API CONFIGURATION
// ========================================
const API_BASE_URL = ''; // Use relative URLs - nginx will proxy to backend

// ========================================
// STATE MANAGEMENT
// ========================================
let currentReportFile = null;

// ========================================
// DOM ELEMENTS
// ========================================
const queryForm = document.getElementById('queryForm');
const submitBtn = document.getElementById('submitBtn');
const downloadBtn = document.getElementById('downloadBtn');
const loadingState = document.getElementById('loadingState');
const resultsSection = document.getElementById('resultsSection');
const errorSection = document.getElementById('errorSection');
const themeToggle = document.getElementById('themeToggle');
const themeIcon = document.getElementById('themeIcon');
const themeText = document.getElementById('themeText');

// ========================================
// THEME MANAGEMENT
// ========================================
function toggleTheme() {
    const isLight = document.body.classList.toggle('light-mode');
    localStorage.setItem('theme', isLight ? 'light' : 'dark');
    updateThemeUI(isLight);
}

function updateThemeUI(isLight) {
    if (isLight) {
        themeIcon.textContent = '🌙';
        themeText.textContent = 'Dark Mode';
    } else {
        themeIcon.textContent = '☀️';
        themeText.textContent = 'Light Mode';
    }
}

// Initialize theme
const savedTheme = localStorage.getItem('theme');
if (savedTheme === 'light') {
    document.body.classList.add('light-mode');
    updateThemeUI(true);
}

themeToggle.addEventListener('click', toggleTheme);

// ========================================
// FLAGGING FOR AUTO DOWNLOAD
// FLAGGING FOR AUTO DOWNLOAD
// ========================================
let shouldAutoDownload = true;

// ========================================
// FORM SUBMISSION
// ========================================
queryForm.addEventListener('submit', async (e) => {
    e.preventDefault();

    const question = document.getElementById('question').value.trim();
    const database = document.getElementById('database').value.trim();

    if (!question || !database) {
        showError('Please fill in all fields');
        return;
    }

    // Reset UI
    hideResults();
    hideError();
    showLoading();

    try {
        console.log('Sending query:', { question, database });

        // Call API to process the query
        const response = await processQuery(question, database);
        console.log('API Response:', response);

        if (response.success) {
            displayResults(response.data);
            currentReportFile = response.data.pdf_file;

            // Auto download if a report file exists
            if (currentReportFile && shouldAutoDownload) {
                console.log('Triggering auto-download for:', currentReportFile);
                triggerDownload(currentReportFile);
            }

        } else {
            showError(response.error || 'An error occurred while processing your query');
        }
    } catch (error) {
        console.error('Critical Error:', error);
        showError('Failed to connect to the server. Please check your network connection or try again.');
    } finally {
        hideLoading();
    }
});

// ========================================
// API CALL FUNCTION
// ========================================
async function processQuery(question, database) {
    try {
        // Call the actual API endpoint
        const response = await fetch(`${API_BASE_URL}/api/analyze`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                question: question,
                database: database
            })
        });

        // Try to parse JSON regardless of status code to get error message
        let data;
        const contentType = response.headers.get("content-type");
        if (contentType && contentType.indexOf("application/json") !== -1) {
            data = await response.json();
            console.log("Parsed JSON data:", data);
        } else {
            console.warn("Response was not JSON:", await response.text());
            data = { success: false, error: `Server returned ${response.status} ${response.statusText}` };
        }

        if (!response.ok) {
            throw new Error(data.error || `HTTP error! status: ${response.status}`);
        }

        return data;

    } catch (error) {
        console.error("Fetch error details:", error);
        return {
            success: false,
            error: error.message || "Failed to fetch data from server"
        };
    }
}

// ========================================
// DISPLAY RESULTS
// ========================================
function displayResults(data) {
    console.log('Displaying results with data:', data);

    // SQL Query - use provided or show placeholder
    const sqlQuery = data.sql_query || 'SQL query generated successfully';
    const sqlCodeBlock = document.querySelector('#sqlQuery code');
    if (sqlCodeBlock) sqlCodeBlock.textContent = sqlQuery;

    // Validation Status
    const statusBadge = document.getElementById('validationStatus');
    const validationResult = data.validation_result || { safe_to_execute: true };
    if (validationResult.safe_to_execute) {
        statusBadge.className = 'status-badge success';
        statusBadge.innerHTML = '✓ Query Validated Successfully';
    } else {
        statusBadge.className = 'status-badge error';
        statusBadge.innerHTML = '✗ Validation Failed';
    }

    // Analysis Summary - use message if analysis not available
    const summary = (data.analysis && data.analysis.summary) || data.message || 'Analysis completed.';
    const analysisEl = document.getElementById('analysisSummary');
    if (analysisEl) analysisEl.textContent = summary;

    // Key Metrics - only show if available
    const metricsContainer = document.getElementById('metricsContainer');
    if (metricsContainer) {
        if (data.analysis && data.analysis.key_metrics && data.analysis.key_metrics.length > 0) {
            const metricsGrid = document.getElementById('metrics');
            if (metricsGrid) {
                metricsGrid.innerHTML = '';
                data.analysis.key_metrics.forEach(metric => {
                    const metricCard = document.createElement('div');
                    metricCard.className = 'metric-card';
                    metricCard.innerHTML = `
                        <div class="metric-label">${metric.metric}</div>
                        <div class="metric-value">
                            ${metric.value}
                            ${metric.unit ? `<span class="metric-unit">${metric.unit}</span>` : ''}
                        </div>
                    `;
                    metricsGrid.appendChild(metricCard);
                });
            }
            metricsContainer.style.display = 'block';
        } else {
            metricsContainer.style.display = 'none';
        }
    }

    // Data Table - only show if available
    const tableContainer = document.getElementById('tableContainer');
    if (tableContainer) {
        if (data.query_results && Array.isArray(data.query_results) && data.query_results.length > 0) {
            const table = document.getElementById('resultsTable');
            if (table) {
                table.innerHTML = '';

                // Generate headers from the first row if available, or just generic ones
                // Ideally, backend should send headers, but if it sends list of lists:
                const headerRow = document.createElement('tr');

                // If query_results is list of lists, we don't have headers unless provided separately
                // Assuming data.query_results contains raw rows.
                // Let's make generic headers or try to infer length
                const numCols = data.query_results[0].length;
                for (let i = 0; i < numCols; i++) {
                    const th = document.createElement('th');
                    th.textContent = `Column ${i + 1}`;
                    headerRow.appendChild(th);
                }
                table.appendChild(headerRow);

                // Data rows
                data.query_results.forEach(row => {
                    const tr = document.createElement('tr');
                    row.forEach(cell => {
                        const td = document.createElement('td');
                        td.textContent = cell;
                        tr.appendChild(td);
                    });
                    table.appendChild(tr);
                });
            }
            tableContainer.style.display = 'block';
        } else {
            tableContainer.style.display = 'none';
        }
    }

    // Process Log
    const processLog = document.getElementById('processLog');
    if (processLog) {
        processLog.innerHTML = '';
        const messages = data.messages || ['Query processed.'];
        messages.forEach(msg => {
            const entry = document.createElement('div');
            entry.className = 'log-entry';
            entry.textContent = msg;
            processLog.appendChild(entry);
        });

        // Add PDF file info to log
        if (data.pdf_file) {
            const pdfEntry = document.createElement('div');
            pdfEntry.className = 'log-entry';
            pdfEntry.innerHTML = `📄 Report generated: <strong>${data.pdf_file}</strong>`;
            processLog.appendChild(pdfEntry);
        }
    }

    // Show results
    if (resultsSection) resultsSection.style.display = 'block';
}

// ========================================
// DOWNLOAD HELPER
// ========================================
async function triggerDownload(filename) {
    if (!filename) return;

    try {
        console.log("Initiating download for:", filename);
        // Fetch the PDF file from the server
        const response = await fetch(`${API_BASE_URL}/api/download/${filename}`);

        if (!response.ok) {
            throw new Error('Download failed with status ' + response.status);
        }

        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
        console.log("Download triggered successfully");
    } catch (error) {
        console.error('Download error:', error);
        // We warn but don't hide results
        alert('Report generated, but automatic download failed. Please click "Download Report" button.');
    }
}

// ========================================
// DOWNLOAD BUTTON EVENT
// ========================================
if (downloadBtn) {
    downloadBtn.addEventListener('click', () => {
        if (!currentReportFile) {
            alert('No report available to download');
            return;
        }
        triggerDownload(currentReportFile);
    });
}

// ========================================
// UI STATE MANAGEMENT
// ========================================
function showLoading() {
    if (loadingState) loadingState.style.display = 'block';
    if (submitBtn) submitBtn.disabled = true;
}

function hideLoading() {
    if (loadingState) loadingState.style.display = 'none';
    if (submitBtn) submitBtn.disabled = false;
}

function showResults() {
    if (resultsSection) resultsSection.style.display = 'block';
}

function hideResults() {
    if (resultsSection) resultsSection.style.display = 'none';
}

function showError(message) {
    if (errorSection) {
        errorSection.style.display = 'block';
        const msgEl = document.getElementById('errorMessage');
        if (msgEl) msgEl.textContent = message;
    }
}

function hideError() {
    if (errorSection) errorSection.style.display = 'none';
}

// ========================================
// INITIALIZATION
// ========================================
document.addEventListener('DOMContentLoaded', () => {
    console.log('NL to SQL Frontend Initialized v2.0');

    // Add smooth scroll behavior
    document.querySelectorAll('a[href^="#"]').forEach(anchor => {
        anchor.addEventListener('click', function (e) {
            e.preventDefault();
            const target = document.querySelector(this.getAttribute('href'));
            if (target) {
                target.scrollIntoView({ behavior: 'smooth' });
            }
        });
    });
});
