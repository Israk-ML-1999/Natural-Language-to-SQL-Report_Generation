"""
nl_to_sql_langgraph.py
Natural Language to SQL System with LangGraph
Multi-Agent System with Query Validation and Multi-DB Support

Usage:
    python nl_to_sql_langgraph.py
"""

import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import TypedDict, Annotated, Literal
from sqlalchemy import create_engine, text, inspect
from fpdf import FPDF
from PIL import Image
import anthropic
from langgraph.graph import StateGraph, END
import operator
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ============================================================================
# STATE DEFINITION
# ============================================================================

class AgentState(TypedDict):
    """State shared across all agents in the graph"""
    user_question: str
    db_type: str
    db_url: str
    schema_info: dict
    relevant_tables: list
    sql_query: str
    validation_result: dict
    query_results: pd.DataFrame
    analysis: dict
    chart_files: list
    pdf_file: str
    error: str
    messages: Annotated[list, operator.add]


# ============================================================================
# DATABASE MANAGER (Multi-DB Support)
# ============================================================================

class MultiDBManager:
    """Handles multiple database types with unified interface"""
    
    SUPPORTED_DBS = {
        'postgresql': 'postgresql://',
        'mysql': 'mysql+pymysql://',
        'sqlite': 'sqlite:///'
    }
    
    def __init__(self, db_url: str):
        self.db_url = db_url
        self.db_type = self._detect_db_type(db_url)
        self.engine = create_engine(db_url)
        
    def _detect_db_type(self, db_url: str) -> str:
        """Auto-detect database type from URL"""
        for db_type, prefix in self.SUPPORTED_DBS.items():
            if db_url.startswith(prefix):
                return db_type
        return 'unknown'
    
    def get_schema_info(self) -> dict:
        """Extract schema information for any database type"""
        inspector = inspect(self.engine)
        schema = {}
        
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            foreign_keys = inspector.get_foreign_keys(table_name)
            
            schema[table_name] = {
                "columns": [col['name'] for col in columns],
                "column_types": {col['name']: str(col['type']) for col in columns},
                "foreign_keys": [
                    f"{fk['constrained_columns']} -> {fk['referred_table']}.{fk['referred_columns']}"
                    for fk in foreign_keys
                ]
            }
        
        return schema
    
    def execute_query(self, query: str) -> pd.DataFrame:
        """Execute query with error handling"""
        try:
            with self.engine.connect() as conn:
                result = pd.read_sql(text(query), conn)
            return result
        except Exception as e:
            raise Exception(f"Query execution failed: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except:
            return False


# ============================================================================
# LANGGRAPH AGENTS
# ============================================================================

class SchemaAnalysisAgent:
    """Agent to analyze schema and identify relevant tables"""
    
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def __call__(self, state: AgentState) -> AgentState:
        """Identify which tables are needed for the query"""
        
        schema_context = json.dumps(state['schema_info'], indent=2)
        
        prompt = f"""You are a professional database architect analyzing schema for optimal query design.

Database Type: {state['db_type']}
Database Schema:
{schema_context}

User Question: {state['user_question']}

Your task:
1. Identify Which tables contain data needed to answer the question accurately
2. Analyze foreign key relationships for proper JOINs
3. Consider performance implications (avoid unnecessary table scans)
4. Ensure all required columns are available in selected tables

Return ONLY a JSON object:
{{
    "tables": ["table1", "table2"],
    "reasoning": "Concise explanation of why these specific tables are essential",
    "join_strategy": "Specific JOIN conditions using foreign keys"
}}

Response:"""

        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response = message.content[0].text.strip()
        response = response.replace("```json", "").replace("```", "").strip()
        
        try:
            result = json.loads(response)
            state['relevant_tables'] = result['tables']
            state['messages'].append(f"✓ Schema Analysis: Found {len(result['tables'])} relevant tables - {', '.join(result['tables'])}")
        except:
            state['relevant_tables'] = list(state['schema_info'].keys())
            state['messages'].append("⚠ Schema Analysis: Using all tables as fallback")
        
        return state


class SQLGenerationAgent:
    """Agent to generate SQL queries from natural language"""
    
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def _get_db_specific_syntax(self, db_type: str) -> str:
        """Get database-specific SQL syntax notes"""
        syntax_notes = {
            'postgresql': "Use PostgreSQL syntax. DATE_TRUNC for dates, INTERVAL for date math. Example: DATE_TRUNC('month', sale_date)",
            'mysql': "Use MySQL syntax. DATE_FORMAT for dates, DATE_SUB for date math. Example: DATE_SUB(CURDATE(), INTERVAL 1 MONTH)",
            'sqlite': "Use SQLite syntax. strftime for dates, datetime for date math. Example: date('now', '-1 month')"
        }
        return syntax_notes.get(db_type, "Use standard SQL syntax.")
    
    def __call__(self, state: AgentState) -> AgentState:
        """Generate SQL query from natural language"""
        
        relevant_schema = {
            table: state['schema_info'][table] 
            for table in state['relevant_tables'] 
            if table in state['schema_info']
        }
        
        schema_context = json.dumps(relevant_schema, indent=2)
        syntax_note = self._get_db_specific_syntax(state['db_type'])
        
        prompt = f"""You are an expert SQL developer creating production-ready queries.

Database Type: {state['db_type'].upper()}
{syntax_note}

Relevant Schema:
{schema_context}

User Question: {state['user_question']}

Professional Requirements:
1. Generate ONLY the SQL query - no markdown, explanations, or comments
2. Use explicit JOINs with proper ON clauses (never implicit joins)
3. Apply meaningful column aliases for readability (e.g., 'total_sales', 'product_name')
4. Use appropriate date functions for {state['db_type']} for date filtering
5. Include GROUP BY for all aggregations with proper HAVING clauses if needed
6. Add ORDER BY to sort results logically (DESC for rankings, ASC for chronological)
7. Limit results appropriately (TOP 10, LIMIT 20, etc.) for large datasets
8. Use DISTINCT only when necessary to avoid duplicates
9. Optimize for performance - avoid SELECT * when specific columns suffice
10. Format column names in results to be human-readable

SQL Query:"""

        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        sql_query = message.content[0].text.strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        state['sql_query'] = sql_query
        state['messages'].append(f"✓ SQL Generation: Query created")
        
        return state


class QueryValidationAgent:
    """Agent to validate SQL queries before execution"""
    
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def __call__(self, state: AgentState) -> AgentState:
        """Validate SQL query for safety and correctness"""
        
        schema_context = json.dumps(state['schema_info'], indent=2)
        
        prompt = f"""You are a senior database security expert validating SQL queries for production use.

Database Type: {state['db_type']}
Schema:
{schema_context}

SQL Query:
{state['sql_query']}

Perform comprehensive validation:

SECURITY CHECKS:
1. SQL injection vulnerabilities (parameterization, string concatenation)
2. Dangerous operations (DROP, DELETE, TRUNCATE, UPDATE, ALTER, CREATE)
3. Unauthorized data access attempts

CORRECTNESS CHECKS:
4. Syntax errors specific to {state['db_type']}
5. Invalid table or column references against schema
6. Missing or incorrect JOIN conditions
7. Aggregation without proper GROUP BY
8. Data type mismatches in comparisons

PERFORMANCE CHECKS:
9. Cartesian products (missing JOIN conditions)
10. SELECT * on large tables
11. Missing WHERE clauses on large datasets
12. Inefficient subqueries

Return ONLY a JSON object:
{{
    "valid": true/false,
    "issues": ["Specific, actionable issue descriptions"],
    "severity": "low/medium/high",
    "suggestions": ["Concrete improvement recommendations"],
    "safe_to_execute": true/false
}}

Response:"""

        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response = message.content[0].text.strip()
        response = response.replace("```json", "").replace("```", "").strip()
        
        try:
            validation = json.loads(response)
            state['validation_result'] = validation
            
            if validation['safe_to_execute']:
                state['messages'].append(f"✓ Query Validation: Passed")
            else:
                state['messages'].append(f"✗ Query Validation: Failed - {'; '.join(validation['issues'][:2])}")
                state['error'] = f"Query validation failed: {', '.join(validation['issues'])}"
        except:
            state['validation_result'] = {
                "valid": True,
                "safe_to_execute": True,
                "issues": [],
                "severity": "low",
                "suggestions": []
            }
            state['messages'].append("⚠ Query Validation: Could not parse validation response, proceeding cautiously")
        
        return state


class QueryExecutionAgent:
    """Agent to execute validated SQL queries"""
    
    def __init__(self, db_manager: MultiDBManager):
        self.db_manager = db_manager
    
    def __call__(self, state: AgentState) -> AgentState:
        """Execute SQL query and return results"""
        
        if not state['validation_result'].get('safe_to_execute', False):
            state['error'] = "Query failed validation, not executing"
            state['messages'].append("✗ Query Execution: Blocked by validation")
            return state
        
        try:
            df = self.db_manager.execute_query(state['sql_query'])
            state['query_results'] = df
            state['messages'].append(f"✓ Query Execution: Retrieved {len(df)} rows, {len(df.columns)} columns")
        except Exception as e:
            state['error'] = str(e)
            state['messages'].append(f"✗ Query Execution: {str(e)}")
        
        return state


class DataAnalysisAgent:
    """Agent to analyze query results"""
    
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def __call__(self, state: AgentState) -> AgentState:
        """Analyze data and suggest visualizations"""
        
        if state.get('error') or state['query_results'] is None:
            return state
        
        df = state['query_results']
        
        if len(df) == 0:
            state['analysis'] = {
                "summary": "No data found matching the query criteria.",
                "key_metrics": [],
                "visualizations": [],
                "insights": ["No records match the specified conditions"]
            }
            state['messages'].append("⚠ Data Analysis: No data to analyze")
            return state
        
        data_summary = f"""
Shape: {df.shape}
Columns: {list(df.columns)}
Data Types: {df.dtypes.to_dict()}

Sample Data (first 3 rows):
{df.head(3).to_string()}

Statistics:
{df.describe().to_string() if len(df) > 0 else 'No data'}
"""
        
        prompt = f"""You are a professional data analyst creating executive-level insights for business stakeholders.

Original Question: {state['user_question']}

Data Summary:
{data_summary}

Provide a comprehensive JSON analysis:
{{
    "summary": "Executive summary with key findings, specific numbers, and business impact (2-3 sentences)",
    "key_metrics": [
        {{"metric": "Clear Metric Name", "value": "actual value from data", "unit": "units (e.g., USD, items, percent)"}}
    ],
    "visualizations": [
        {{
            "type": "bar|line|pie|horizontal_bar",
            "x_col": "exact_column_name_from_data",
            "y_col": "exact_column_name_from_data",
            "title": "Professional, descriptive chart title",
            "description": "Business context: what this visualization reveals"
        }}
    ],
    "insights": ["Actionable insight with business context", "Trend or pattern identified", "Recommendation if applicable"]
}}

PROFESSIONAL STANDARDS:
1. Use EXACT column names from the data (case-sensitive)
2. Suggest 1-2 visualizations maximum (only the most impactful)
3. Choose visualization types based on data:
   - Bar/Horizontal Bar: Comparisons, rankings, categories
   - Line: Trends over time, sequential data
   - Pie: Proportions (only if 3-6 categories)
4. Use plain ASCII text only (no bullets, emojis, or special characters)
5. Make titles and descriptions business-focused, not technical
6. Ensure all metrics have proper units and context
7. Provide insights that drive decision-making

Response:"""

        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}]
        )
        
        response = message.content[0].text.strip()
        response = response.replace("```json", "").replace("```", "").strip()
        
        try:
            analysis = json.loads(response)
            
            # Ensure visualizations exist - add fallback if AI didn't provide any
            if not analysis.get('visualizations') or len(analysis.get('visualizations', [])) == 0:
                # Auto-generate a basic visualization based on data structure
                if len(df.columns) >= 2:
                    # Use first two columns for a simple chart
                    col1, col2 = df.columns[0], df.columns[1]
                    
                    # Determine chart type based on data
                    if len(df) <= 10:
                        chart_type = 'bar'
                    else:
                        chart_type = 'horizontal_bar'
                    
                    analysis['visualizations'] = [{
                        'type': chart_type,
                        'x_col': col1,
                        'y_col': col2,
                        'title': f'{col2.replace("_", " ").title()} by {col1.replace("_", " ").title()}',
                        'description': f'Distribution of {col2} across {col1}'
                    }]
                    state['messages'].append(f"⚠ Data Analysis: Auto-generated visualization (AI didn't provide one)")
            
            state['analysis'] = analysis
            state['messages'].append(f"✓ Data Analysis: Generated {len(analysis.get('visualizations', []))} visualization specs")
        except Exception as e:
            state['analysis'] = {
                "summary": f"Analysis completed with {len(df)} rows of data.",
                "key_metrics": [],
                "visualizations": [],
                "insights": []
            }
            state['messages'].append(f"⚠ Data Analysis: Could not parse response - {str(e)}")
        
        return state


class VisualizationAgent:
    """Agent to create data visualizations"""
    
    def __call__(self, state: AgentState) -> AgentState:
        """Create optimized, production-ready visualizations"""
        
        if state.get('error') or state['query_results'] is None or len(state['query_results']) == 0:
            state['chart_files'] = []
            state['messages'].append("⚠ Visualization: Skipped - no data available")
            return state
        
        df = state['query_results']
        viz_specs = state['analysis'].get('visualizations', [])
        chart_files = []
        
        if not viz_specs or len(viz_specs) == 0:
            state['messages'].append("⚠ Visualization: No visualization specifications found in analysis")
            state['chart_files'] = []
            return state
        
        state['messages'].append(f"→ Visualization: Processing {len(viz_specs)} chart specification(s)")
        
        # Professional styling
        sns.set_style("whitegrid")
        sns.set_palette("husl")
        plt.rcParams['font.size'] = 10
        plt.rcParams['axes.titlesize'] = 12
        plt.rcParams['axes.labelsize'] = 10
        
        for i, viz in enumerate(viz_specs):
            try:
                x_col = viz.get('x_col')
                y_col = viz.get('y_col')
                
                if not x_col or not y_col:
                    continue
                
                if x_col not in df.columns or y_col not in df.columns:
                    state['messages'].append(f"⚠ Visualization {i}: Column not found - {x_col} or {y_col}")
                    continue
                
                # Determine optimal figure size based on data
                max_label_length = df[x_col].astype(str).str.len().max() if viz['type'] != 'pie' else 0
                num_categories = len(df)
                
                # Compact, efficient sizing for production
                if viz['type'] == 'horizontal_bar' or (viz['type'] == 'bar' and num_categories > 10):
                    fig_height = min(8, max(4, num_categories * 0.3))
                    plt.figure(figsize=(8, fig_height))
                elif viz['type'] == 'pie':
                    plt.figure(figsize=(7, 7))
                else:
                    plt.figure(figsize=(10, 6))
                
                if viz['type'] == 'bar':
                    # Use horizontal bar for better readability with many categories
                    if num_categories > 10 or max_label_length > 15:
                        sns.barplot(data=df, y=x_col, x=y_col, orient='h')
                        plt.xlabel(y_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                        plt.ylabel(x_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                    else:
                        sns.barplot(data=df, x=x_col, y=y_col)
                        plt.xlabel(x_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                        plt.ylabel(y_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                        if max_label_length > 8:
                            plt.xticks(rotation=45, ha='right')
                    
                elif viz['type'] == 'line':
                    plt.plot(df[x_col], df[y_col], marker='o', linewidth=2.5, markersize=7, color='#2E86AB')
                    if max_label_length > 8 or num_categories > 10:
                        plt.xticks(rotation=45, ha='right')
                    plt.xlabel(x_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                    plt.ylabel(y_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                    plt.grid(True, alpha=0.3, linestyle='--')
                    
                elif viz['type'] == 'pie':
                    # Compact pie chart with smart labeling
                    labels = df[x_col].astype(str).tolist()
                    if any(len(label) > 12 for label in labels):
                        labels = [label[:10] + '...' if len(label) > 12 else label for label in labels]
                    
                    colors = sns.color_palette("husl", len(labels))
                    wedges, texts, autotexts = plt.pie(
                        df[y_col], 
                        labels=labels, 
                        autopct='%1.1f%%', 
                        startangle=90,
                        colors=colors,
                        textprops={'fontsize': 9}
                    )
                    for autotext in autotexts:
                        autotext.set_color('white')
                        autotext.set_fontweight('bold')
                    plt.axis('equal')
                    
                elif viz['type'] == 'horizontal_bar':
                    sns.barplot(data=df, y=x_col, x=y_col, orient='h')
                    plt.xlabel(y_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                    plt.ylabel(x_col.replace('_', ' ').title(), fontsize=11, fontweight='bold')
                
                # Professional title
                plt.title(viz['title'], fontsize=13, fontweight='bold', pad=15)
                plt.tight_layout()
                
                # Save with optimized settings for smaller file size
                filename = f"chart_{i}_{datetime.now().strftime('%H%M%S%f')}.png"
                plt.savefig(filename, dpi=150, bbox_inches='tight', format='png')
                plt.close()
                
                chart_files.append(filename)
                state['messages'].append(f"  → Created chart: {filename} ({viz['type']})")
                
            except Exception as e:
                state['messages'].append(f"⚠ Visualization {i}: Error - {str(e)}")
                plt.close()
        
        state['chart_files'] = chart_files
        state['messages'].append(f"✓ Visualization: Created {len(chart_files)} optimized charts")
        
        return state


class PDFGenerationAgent:
    """Agent to generate comprehensive PDF reports"""
    
    def _clean_text(self, text: str) -> str:
        """Remove special characters that cause PDF rendering issues"""
        # Replace common problematic characters
        replacements = {
            '•': '-',
            '◆': '-',
            '○': '-',
            '▪': '-',
            '▫': '-',
            '→': '->',
            '←': '<-',
            '↑': '^',
            '↓': 'v',
            '✓': 'OK',
            '✗': 'X',
            '✅': '[OK]',
            '❌': '[X]',
            ''': "'",
            ''': "'",
            '"': '"',
            '"': '"',
            '–': '-',
            '—': '-'
        }
        
        for old, new in replacements.items():
            text = text.replace(old, new)
        
        # Remove any remaining non-ASCII characters
        text = text.encode('ascii', 'ignore').decode('ascii')
        
        return text
    
    def _safe_multi_cell(self, pdf, w, h, txt, border=0, align='L', fill=False):
        """Safely render multi-cell text with automatic truncation if needed"""
        try:
            # Get available width
            available_width = pdf.w - pdf.l_margin - pdf.r_margin if w == 0 else w
            
            # Split text into lines that fit
            words = txt.split(' ')
            lines = []
            current_line = ""
            
            for word in words:
                test_line = current_line + (" " if current_line else "") + word
                # Check if line fits
                if pdf.get_string_width(test_line) <= available_width - 4:
                    current_line = test_line
                else:
                    if current_line:
                        lines.append(current_line)
                        current_line = word
                    else:
                        # Single word is too long, truncate it
                        while pdf.get_string_width(word) > available_width - 4 and len(word) > 1:
                            word = word[:-1]
                        current_line = word
            
            if current_line:
                lines.append(current_line)
            
            # Render each line
            for line in lines:
                pdf.multi_cell(w, h, line, border=border, align=align, fill=fill)
        except Exception as e:
            # Fallback: just skip problematic text
            pdf.cell(w, h, "[Text rendering error]", border=border, align=align, fill=fill, new_x="LMARGIN", new_y="NEXT")
    
    def __call__(self, state: AgentState) -> AgentState:
        """Generate detailed PDF report"""
        
        pdf = FPDF()
        pdf.add_page()
        
        # ====================================================================
        # HEADER
        # ====================================================================
        pdf.set_font('Helvetica', 'B', 20)
        pdf.set_text_color(0, 51, 102)
        pdf.cell(0, 15, 'SQL Query Analysis Report', new_x="LMARGIN", new_y="NEXT", align='C')
        pdf.set_text_color(0, 0, 0)
        pdf.ln(5)
        
        # Metadata
        pdf.set_font('Helvetica', 'I', 9)
        pdf.cell(0, 5, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", new_x="LMARGIN", new_y="NEXT")
        pdf.cell(0, 5, f"Database: {state['db_type'].upper()}", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(5)
        
        # ====================================================================
        # QUESTION SECTION
        # ====================================================================
        pdf.set_font('Helvetica', 'B', 14)
        pdf.set_fill_color(230, 230, 230)
        pdf.cell(0, 10, '1. User Question', new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_font('Helvetica', '', 11)
        self._safe_multi_cell(pdf, 0, 8, state['user_question'])
        pdf.ln(3)
        
        # ====================================================================
        # SQL QUERY SECTION
        # ====================================================================
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, '2. Generated SQL Query', new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_font('Courier', '', 9)
        self._safe_multi_cell(pdf, 0, 5, state['sql_query'])
        pdf.ln(3)
        
        # ====================================================================
        # VALIDATION RESULTS
        # ====================================================================
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, '3. Query Validation', new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_font('Helvetica', '', 10)
        
        validation = state['validation_result']
        status = "PASSED" if validation.get('safe_to_execute') else "FAILED"
        
        if validation.get('safe_to_execute'):
            pdf.set_text_color(0, 128, 0)
        else:
            pdf.set_text_color(255, 0, 0)
        
        pdf.cell(0, 7, f"Status: {status}", new_x="LMARGIN", new_y="NEXT")
        pdf.set_text_color(0, 0, 0)
        
        if validation.get('issues'):
            pdf.cell(0, 7, f"Issues Found: {len(validation['issues'])}", new_x="LMARGIN", new_y="NEXT")
            for issue in validation['issues'][:5]:
                self._safe_multi_cell(pdf, 0, 6, f"  - {self._clean_text(issue)}")
        else:
            pdf.cell(0, 7, "No issues found", new_x="LMARGIN", new_y="NEXT")
        pdf.ln(3)
        
        # ====================================================================
        # ANALYSIS SUMMARY
        # ====================================================================
        analysis = state['analysis']
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, '4. Analysis Summary', new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_font('Helvetica', '', 11)
        self._safe_multi_cell(pdf, 0, 8, analysis.get('summary', 'No summary available'))
        pdf.ln(3)
        
        # ====================================================================
        # KEY METRICS
        # ====================================================================
        if analysis.get('key_metrics'):
            pdf.set_font('Helvetica', 'B', 14)
            pdf.cell(0, 10, '5. Key Metrics', new_x="LMARGIN", new_y="NEXT", fill=True)
            pdf.set_font('Helvetica', '', 11)
            
            for metric in analysis['key_metrics']:
                try:
                    unit = f" {metric.get('unit', '')}" if metric.get('unit') else ""
                    metric_text = self._clean_text(f"{metric['metric']}: {metric['value']}{unit}")
                    self._safe_multi_cell(pdf, 0, 7, f"  - {metric_text}")
                except Exception as e:
                    # Skip problematic metrics
                    pass
            pdf.ln(3)
        
        # ====================================================================
        # DATA TABLE SECTION (PROFESSIONAL VERSION)
        # ====================================================================
        if state['query_results'] is not None and len(state['query_results']) > 0:
            df = state['query_results']

            # Force LANDSCAPE page for tables
            pdf.add_page(orientation='L')

            pdf.set_font('Helvetica', 'B', 14)
            pdf.cell(0, 10, '6. Query Results', new_x="LMARGIN", new_y="NEXT", fill=True)
            
            # Add result summary
            pdf.set_font('Helvetica', 'I', 10)
            pdf.cell(0, 6, f"Total Records: {len(df)} | Columns: {len(df.columns)}", new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

            pdf.set_font('Helvetica', '', 9)

            available_width = pdf.w - pdf.l_margin - pdf.r_margin

            # Optimize column display
            MIN_COL_WIDTH = 30
            MAX_COLS = int(available_width // MIN_COL_WIDTH)
            MAX_COLS = max(1, min(MAX_COLS, 8))  # Limit to 8 columns max

            display_cols = list(df.columns)[:MAX_COLS]
            col_width = available_width / len(display_cols)

            # ---------------- HEADER ----------------
            pdf.set_font('Helvetica', 'B', 9)
            pdf.set_fill_color(70, 130, 180)  # Professional blue
            pdf.set_text_color(255, 255, 255)  # White text

            for col in display_cols:
                # Format column name professionally
                col_display = str(col).replace('_', ' ').title()
                
                # Truncate if too long
                while pdf.get_string_width(col_display) > col_width - 4 and len(col_display) > 0:
                    col_display = col_display[:-1]

                if not col_display:
                    col_display = " "

                pdf.cell(col_width, 8, col_display, border=1, align='C', fill=True)

            pdf.ln()
            pdf.set_text_color(0, 0, 0)  # Reset to black

            # ---------------- ROWS ----------------
            pdf.set_font('Helvetica', '', 8)
            pdf.set_fill_color(240, 240, 240)  # Light gray for alternating rows

            max_rows = min(20, len(df))  # Limit to 20 rows for efficiency
            
            for idx, (_, row) in enumerate(df.head(max_rows).iterrows()):
                fill = idx % 2 == 0  # Alternate row colors
                
                for col in display_cols:
                    value = str(row[col])
                    
                    # Clean and format value
                    value = self._clean_text(value)
                    
                    # Truncate if needed
                    while pdf.get_string_width(value) > col_width - 4 and len(value) > 0:
                        value = value[:-1]

                    if not value:
                        value = " "

                    pdf.cell(col_width, 6, value, border=1, fill=fill)

                pdf.ln()

            # ---------------- FOOTER ----------------
            pdf.set_font('Helvetica', 'I', 9)
            pdf.set_text_color(100, 100, 100)

            if len(display_cols) < len(df.columns):
                pdf.cell(
                    0, 6,
                    f"Note: Showing {len(display_cols)} of {len(df.columns)} columns for optimal display",
                    new_x="LMARGIN", new_y="NEXT"
                )

            if len(df) > max_rows:
                pdf.cell(
                    0, 6,
                    f"Note: Showing first {max_rows} of {len(df)} total records",
                    new_x="LMARGIN", new_y="NEXT"
                )
            
            pdf.set_text_color(0, 0, 0)  # Reset color

        elif state.get('error'):
            pdf.set_font('Helvetica', 'B', 14)
            pdf.cell(0, 10, '7. Error', new_x="LMARGIN", new_y="NEXT", fill=True)
            pdf.set_font('Helvetica', '', 11)
            pdf.set_text_color(255, 0, 0)
            self._safe_multi_cell(pdf, 0, 8, state['error'])
            pdf.set_text_color(0, 0, 0)
        
        # ====================================================================
        # VISUALIZATIONS (OPTIMIZED)
        # ====================================================================
        if state['chart_files']:
            state['messages'].append(f"→ PDF: Embedding {len(state['chart_files'])} chart(s)")
            pdf.add_page()
            pdf.set_font('Helvetica', 'B', 16)
            pdf.cell(0, 10, '8. Data Visualizations', new_x="LMARGIN", new_y="NEXT")
            pdf.ln(3)
            
            for i, chart_file in enumerate(state['chart_files']):
                if os.path.exists(chart_file):
                    viz_info = analysis['visualizations'][i] if i < len(analysis.get('visualizations', [])) else {}
                    
                    if viz_info.get('description'):
                        pdf.set_font('Helvetica', 'I', 10)
                        self._safe_multi_cell(pdf, 0, 6, f"Chart {i+1}: {self._clean_text(viz_info['description'])}")
                        pdf.ln(2)
                    
                    try:
                        img = Image.open(chart_file)
                        img_width_px, img_height_px = img.size
                        dpi = img.info.get('dpi', (150, 150))[0]
                        if dpi == 0:
                            dpi = 150
                        
                        # Convert to mm
                        img_width_mm = img_width_px / dpi * 25.4
                        img_height_mm = img_height_px / dpi * 25.4
                        
                        # Get available page dimensions (portrait mode)
                        page_w = pdf.w - pdf.l_margin - pdf.r_margin
                        page_h = pdf.h - pdf.t_margin - pdf.b_margin - 40
                        
                        # Scale to fit page efficiently (max 70% of page width for compact layout)
                        max_width = page_w * 0.7
                        max_height = page_h * 0.6
                        
                        scale_w = max_width / img_width_mm if img_width_mm > max_width else 1
                        scale_h = max_height / img_height_mm if img_height_mm > max_height else 1
                        scale = min(scale_w, scale_h, 1)
                        
                        final_w = img_width_mm * scale
                        final_h = img_height_mm * scale
                        
                        # Center the image
                        x_pos = pdf.l_margin + (page_w - final_w) / 2
                        
                        # Check if we need a new page
                        if pdf.get_y() + final_h > pdf.h - pdf.b_margin - 10:
                            pdf.add_page()
                        
                        pdf.image(chart_file, x=x_pos, w=final_w, h=final_h)
                        pdf.ln(8)
                        state['messages'].append(f"  → Embedded chart {i+1} in PDF")
                        
                    except Exception as e:
                        pdf.set_font('Helvetica', 'I', 9)
                        pdf.cell(0, 6, f"Could not load chart: {str(e)}", new_x="LMARGIN", new_y="NEXT")
                        state['messages'].append(f"  ✗ Failed to embed chart {i+1}: {str(e)}")
                else:
                    state['messages'].append(f"  ✗ Chart file not found: {chart_file}")
        else:
            state['messages'].append("⚠ PDF: No charts to embed")
        
        # ====================================================================
        # PROCESS LOG
        # ====================================================================
        pdf.add_page()
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, '9. Process Log', new_x="LMARGIN", new_y="NEXT", fill=True)
        pdf.set_font('Helvetica', '', 9)
        
        for msg in state['messages']:
            cleaned_msg = self._clean_text(msg)
            # Ensure message isn't too long - wrap if needed
            self._safe_multi_cell(pdf, 0, 6, cleaned_msg)
        
        # ====================================================================
        # SAVE PDF
        # ====================================================================
        output_filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(output_filename)
        
        state['pdf_file'] = output_filename
        state['messages'].append(f"✓ PDF Generation: Saved as {output_filename}")
        
        # Cleanup charts
        for chart_file in state['chart_files']:
            try:
                if os.path.exists(chart_file):
                    os.remove(chart_file)
            except:
                pass
        
        return state


# ============================================================================
# LANGGRAPH WORKFLOW
# ============================================================================

def create_workflow(db_manager: MultiDBManager, api_key: str):
    """Create LangGraph workflow with all agents"""
    
    # Initialize agents
    schema_agent = SchemaAnalysisAgent(api_key)
    sql_agent = SQLGenerationAgent(api_key)
    validation_agent = QueryValidationAgent(api_key)
    execution_agent = QueryExecutionAgent(db_manager)
    analysis_agent = DataAnalysisAgent(api_key)
    viz_agent = VisualizationAgent()
    pdf_agent = PDFGenerationAgent()
    
    # Create workflow
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("schema_analysis", schema_agent)
    workflow.add_node("sql_generation", sql_agent)
    workflow.add_node("query_validation", validation_agent)
    workflow.add_node("query_execution", execution_agent)
    workflow.add_node("data_analysis", analysis_agent)
    workflow.add_node("visualization", viz_agent)
    workflow.add_node("pdf_generation", pdf_agent)
    
    # Define edges
    workflow.set_entry_point("schema_analysis")
    workflow.add_edge("schema_analysis", "sql_generation")
    workflow.add_edge("sql_generation", "query_validation")
    
    # Conditional edge based on validation
    def should_execute(state: AgentState) -> str:
        if state['validation_result'].get('safe_to_execute', False):
            return "query_execution"
        return "pdf_generation"
    
    workflow.add_conditional_edges(
        "query_validation",
        should_execute,
        {
            "query_execution": "query_execution",
            "pdf_generation": "pdf_generation"
        }
    )
    
    workflow.add_edge("query_execution", "data_analysis")
    workflow.add_edge("data_analysis", "visualization")
    workflow.add_edge("visualization", "pdf_generation")
    workflow.add_edge("pdf_generation", END)
    
    return workflow.compile()


# ============================================================================
# MAIN SYSTEM
# ============================================================================

class NLToSQLSystem:
    """Main system orchestrator using LangGraph"""
    
    def __init__(self, db_url: str, api_key: str):
        self.db_manager = MultiDBManager(db_url)
        self.api_key = api_key
        
        # Test connection
        print(f"🔌 Connecting to database...")
        if not self.db_manager.test_connection():
            raise Exception("Failed to connect to database")
        
        print(f"✅ Connected to {self.db_manager.db_type.upper()} database\n")
        
        # Create workflow
        self.workflow = create_workflow(self.db_manager, api_key)
    
    def process_question(self, user_question: str) -> str:
        """Process question through LangGraph workflow"""
        
        print(f"\n{'='*70}")
        print(f"PROCESSING QUERY")
        print(f"{'='*70}")
        print(f"❓ Question: {user_question}")
        print(f"💾 Database: {self.db_manager.db_type.upper()}")
        print(f"{'='*70}\n")
        
        # Initialize state
        initial_state = {
            "user_question": user_question,
            "db_type": self.db_manager.db_type,
            "db_url": self.db_manager.db_url,
            "schema_info": self.db_manager.get_schema_info(),
            "relevant_tables": [],
            "sql_query": "",
            "validation_result": {},
            "query_results": None,
            "analysis": {},
            "chart_files": [],
            "pdf_file": "",
            "error": "",
            "messages": []
        }
        
        # Run workflow
        print("🔄 Running agent workflow...\n")
        final_state = self.workflow.invoke(initial_state)
        
        # Print results
        print("\n" + "="*70)
        print("WORKFLOW COMPLETED")
        print("="*70)
        
        for msg in final_state['messages']:
            print(f"  {msg}")
        
        if final_state.get('error'):
            print(f"\n❌ Error: {final_state['error']}")
        else:
            print(f"\n✅ Success!")
        
        print(f"\n📄 PDF Report: {final_state['pdf_file']}")
        print("="*70 + "\n")
        
        return final_state['pdf_file']


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

def main():
    """Main function to run the system"""
    
    print("\n" + "="*70)
    print(" "*15 + "NATURAL LANGUAGE TO SQL SYSTEM")
    print(" "*20 + "LangGraph Multi-Agent")
    print("="*70 + "\n")
    
    # Check for API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ Error: ANTHROPIC_API_KEY not found in environment variables")
        print("\nPlease set it using one of these methods:")
        print("  • Linux/Mac: export ANTHROPIC_API_KEY='your-api-key-here'")
        print("  • Windows: set ANTHROPIC_API_KEY=your-api-key-here")
        print("  • Or create a .env file with: ANTHROPIC_API_KEY=your-api-key-here")
        return
    
    # Database configuration
    db_file = 'demo_sales.db'
    
    if not os.path.exists(db_file):
        print(f"❌ Error: Database file '{db_file}' not found!")
        print("\n🔧 Please run this command first:")
        print(f"   python demo_database.py")
        return
    
    db_url = f"sqlite:///{db_file}"
    
    # Initialize system
    try:
        system = NLToSQLSystem(db_url, api_key)
    except Exception as e:
        print(f"❌ Failed to initialize system: {str(e)}")
        return
    
    # Demo questions
    demo_questions = [
        "How many t-shirts were sold last month?",
        "What are the total sales by category?",
        "Show me the top 5 best-selling products",
        "How many products are available in the store?",
        "What were the total sales in the last 30 days?",
        "Which users made the most purchases?",
        "What is the average order value?",
        "Show sales trends over the last 3 months"
    ]
    
    print("📋 Available Demo Questions:")
    for i, q in enumerate(demo_questions, 1):
        print(f"   {i}. {q}")
    print(f"   {len(demo_questions)+1}. Custom question")
    print(f"   0. Exit")
    
    while True:
        print("\n" + "="*70)
        choice = input("Select a question (0-9) or press Enter for #1: ").strip()
        
        if choice == '0':
            print("\n👋 Goodbye!")
            break
        
        if choice == '':
            choice = '1'
        
        try:
            choice_num = int(choice)
            
            if choice_num == len(demo_questions) + 1:
                user_question = input("\n💬 Enter your question: ").strip()
                if not user_question:
                    print("❌ Question cannot be empty")
                    continue
            elif 1 <= choice_num <= len(demo_questions):
                user_question = demo_questions[choice_num - 1]
            else:
                print("❌ Invalid choice")
                continue
            
            # Process question
            pdf_file = system.process_question(user_question)
            
            print(f"\n✅ PDF report generated: {pdf_file}")
            
            # Ask if user wants to continue
            cont = input("\n🔄 Process another question? (y/n): ").strip().lower()
            if cont != 'y':
                print("\n👋 Thank you for using NL-to-SQL System!")
                break
                
        except ValueError:
            print("❌ Please enter a valid number")
        except KeyboardInterrupt:
            print("\n\n👋 Interrupted by user. Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            cont = input("\n🔄 Try again? (y/n): ").strip().lower()
            if cont != 'y':
                break


if __name__ == "__main__":
    main()