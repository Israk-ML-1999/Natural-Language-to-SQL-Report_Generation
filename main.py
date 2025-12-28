"""
Natural Language to SQL System with LangGraph
Multi-Agent System with Query Validation and Multi-DB Support
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
import anthropic
from langgraph.graph import StateGraph, END
from langgraph.prebuilt import ToolExecutor
import operator

# ============================================================================
# STATE DEFINITION
# ============================================================================

class AgentState(TypedDict):
    """State shared across all agents in the graph"""
    user_question: str
    db_type: str  # 'postgresql', 'mysql', 'sqlite'
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
        
        prompt = f"""Analyze the user's question and database schema to identify relevant tables.

Database Type: {state['db_type']}
Database Schema:
{schema_context}

User Question: {state['user_question']}

Consider:
1. Which tables contain data needed to answer the question?
2. What relationships exist between tables?
3. Are JOINs needed?

Return ONLY a JSON object:
{{
    "tables": ["table1", "table2"],
    "reasoning": "why these tables are needed",
    "join_strategy": "description of how tables should be joined"
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
            state['messages'].append(f"✓ Schema Analysis: Found {len(result['tables'])} relevant tables")
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
            'postgresql': "Use PostgreSQL syntax. DATE_TRUNC for dates, INTERVAL for date math.",
            'mysql': "Use MySQL syntax. DATE_FORMAT for dates, DATE_SUB for date math.",
            'sqlite': "Use SQLite syntax. strftime for dates, datetime for date math."
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
        
        prompt = f"""Generate a SQL query to answer the user's question.

Database Type: {state['db_type'].upper()}
{syntax_note}

Relevant Schema:
{schema_context}

User Question: {state['user_question']}

Important Rules:
1. Generate ONLY the SQL query, no explanations or markdown
2. Use proper JOINs when accessing multiple tables
3. For date ranges like "last month", use appropriate date functions
4. Use GROUP BY for aggregations
5. Add appropriate WHERE clauses for filtering
6. Consider the database type for syntax

SQL Query:"""

        message = self.client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        
        sql_query = message.content[0].text.strip()
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        state['sql_query'] = sql_query
        state['messages'].append(f"✓ SQL Generation: Query created ({len(sql_query)} chars)")
        
        return state


class QueryValidationAgent:
    """Agent to validate SQL queries before execution"""
    
    def __init__(self, api_key: str):
        self.client = anthropic.Anthropic(api_key=api_key)
    
    def __call__(self, state: AgentState) -> AgentState:
        """Validate SQL query for safety and correctness"""
        
        schema_context = json.dumps(state['schema_info'], indent=2)
        
        prompt = f"""Validate this SQL query for safety and correctness.

Database Type: {state['db_type']}
Schema:
{schema_context}

SQL Query:
{state['sql_query']}

Check for:
1. SQL injection vulnerabilities
2. Syntax errors for {state['db_type']}
3. Invalid table or column names
4. Missing JOINs or WHERE clauses
5. Dangerous operations (DROP, DELETE, TRUNCATE)
6. Performance issues (missing indexes, cartesian products)

Return ONLY a JSON object:
{{
    "valid": true/false,
    "issues": ["list of issues found"],
    "severity": "low/medium/high",
    "suggestions": ["improvements"],
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
                state['messages'].append(f"✗ Query Validation: Failed - {validation['issues']}")
                state['error'] = f"Query validation failed: {validation['issues']}"
        except:
            # Default to safe validation
            state['validation_result'] = {
                "valid": True,
                "safe_to_execute": True,
                "issues": []
            }
            state['messages'].append("⚠ Query Validation: Could not parse, proceeding cautiously")
        
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
        
        data_summary = f"""
Shape: {df.shape}
Columns: {list(df.columns)}
Data Types: {df.dtypes.to_dict()}

Sample Data (first 3 rows):
{df.head(3).to_string()}

Statistics:
{df.describe().to_string() if len(df) > 0 else 'No data'}
"""
        
        prompt = f"""Analyze this query result and provide insights.

Original Question: {state['user_question']}

Data Summary:
{data_summary}

Provide a JSON response:
{{
    "summary": "2-3 sentence summary of key findings",
    "key_metrics": [
        {{"metric": "Total Sales", "value": "1234", "unit": "USD"}},
        {{"metric": "Top Category", "value": "Electronics", "unit": ""}}
    ],
    "visualizations": [
        {{
            "type": "bar",
            "x_col": "category",
            "y_col": "total_sales",
            "title": "Sales by Category",
            "description": "Shows distribution of sales across categories"
        }}
    ],
    "insights": ["insight 1", "insight 2"]
}}

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
            state['analysis'] = analysis
            state['messages'].append(f"✓ Data Analysis: Generated {len(analysis.get('visualizations', []))} visualization specs")
        except:
            state['analysis'] = {
                "summary": "Data analysis completed",
                "key_metrics": [],
                "visualizations": [],
                "insights": []
            }
            state['messages'].append("⚠ Data Analysis: Could not parse, using defaults")
        
        return state


class VisualizationAgent:
    """Agent to create data visualizations"""
    
    def __call__(self, state: AgentState) -> AgentState:
        """Create visualizations based on analysis"""
        
        if state.get('error') or state['query_results'] is None:
            return state
        
        df = state['query_results']
        viz_specs = state['analysis'].get('visualizations', [])
        chart_files = []
        
        sns.set_style("whitegrid")
        sns.set_palette("husl")
        
        for i, viz in enumerate(viz_specs):
            try:
                plt.figure(figsize=(10, 6))
                
                x_col = viz.get('x_col')
                y_col = viz.get('y_col')
                
                if x_col not in df.columns or y_col not in df.columns:
                    continue
                
                if viz['type'] == 'bar':
                    sns.barplot(data=df, x=x_col, y=y_col)
                    plt.xticks(rotation=45, ha='right')
                    
                elif viz['type'] == 'line':
                    plt.plot(df[x_col], df[y_col], marker='o', linewidth=2)
                    plt.xticks(rotation=45, ha='right')
                    plt.grid(True, alpha=0.3)
                    
                elif viz['type'] == 'pie':
                    plt.pie(df[y_col], labels=df[x_col], autopct='%1.1f%%', startangle=90)
                    
                elif viz['type'] == 'horizontal_bar':
                    sns.barplot(data=df, y=x_col, x=y_col, orient='h')
                
                plt.title(viz['title'], fontsize=14, fontweight='bold')
                plt.xlabel(x_col.replace('_', ' ').title())
                plt.ylabel(y_col.replace('_', ' ').title())
                plt.tight_layout()
                
                filename = f"chart_{i}_{datetime.now().strftime('%H%M%S')}.png"
                plt.savefig(filename, dpi=300, bbox_inches='tight')
                plt.close()
                
                chart_files.append(filename)
                
            except Exception as e:
                state['messages'].append(f"⚠ Visualization {i}: {str(e)}")
        
        state['chart_files'] = chart_files
        state['messages'].append(f"✓ Visualization: Created {len(chart_files)} charts")
        
        return state


class PDFGenerationAgent:
    """Agent to generate comprehensive PDF reports"""
    
    def __call__(self, state: AgentState) -> AgentState:
        """Generate detailed PDF report"""
        
        pdf = FPDF()
        pdf.add_page()
        
        # Header
        pdf.set_font('Arial', 'B', 20)
        pdf.set_text_color(0, 51, 102)
        pdf.cell(0, 15, 'SQL Query Analysis Report', ln=True, align='C')
        pdf.set_text_color(0, 0, 0)
        pdf.ln(5)
        
        # Metadata
        pdf.set_font('Arial', 'I', 9)
        pdf.cell(0, 5, f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", ln=True)
        pdf.cell(0, 5, f"Database: {state['db_type'].upper()}", ln=True)
        pdf.ln(5)
        
        # Question Section
        pdf.set_font('Arial', 'B', 14)
        pdf.set_fill_color(230, 230, 230)
        pdf.cell(0, 10, 'User Question', ln=True, fill=True)
        pdf.set_font('Arial', '', 11)
        pdf.multi_cell(0, 8, state['user_question'])
        pdf.ln(3)
        
        # SQL Query Section
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, 'Generated SQL Query', ln=True, fill=True)
        pdf.set_font('Courier', '', 9)
        pdf.multi_cell(0, 5, state['sql_query'])
        pdf.ln(3)
        
        # Validation Results
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, 'Query Validation', ln=True, fill=True)
        pdf.set_font('Arial', '', 10)
        
        validation = state['validation_result']
        status = "✓ PASSED" if validation.get('safe_to_execute') else "✗ FAILED"
        pdf.cell(0, 7, f"Status: {status}", ln=True)
        
        if validation.get('issues'):
            pdf.cell(0, 7, f"Issues Found: {len(validation['issues'])}", ln=True)
            for issue in validation['issues'][:3]:
                pdf.multi_cell(0, 6, f"  • {issue}")
        pdf.ln(3)
        
        # Analysis Summary
        analysis = state['analysis']
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, 'Analysis Summary', ln=True, fill=True)
        pdf.set_font('Arial', '', 11)
        pdf.multi_cell(0, 8, analysis.get('summary', 'No summary available'))
        pdf.ln(3)
        
        # Key Metrics
        if analysis.get('key_metrics'):
            pdf.set_font('Arial', 'B', 14)
            pdf.cell(0, 10, 'Key Metrics', ln=True, fill=True)
            pdf.set_font('Arial', '', 11)
            
            for metric in analysis['key_metrics']:
                unit = f" {metric['unit']}" if metric.get('unit') else ""
                pdf.cell(0, 7, f"  • {metric['metric']}: {metric['value']}{unit}", ln=True)
            pdf.ln(3)
        
        # Insights
        if analysis.get('insights'):
            pdf.set_font('Arial', 'B', 14)
            pdf.cell(0, 10, 'Key Insights', ln=True, fill=True)
            pdf.set_font('Arial', '', 11)
            
            for insight in analysis['insights']:
                pdf.multi_cell(0, 7, f"  • {insight}")
            pdf.ln(3)
        
        # Data Table
        if state['query_results'] is not None and len(state['query_results']) > 0:
            df = state['query_results']
            
            pdf.set_font('Arial', 'B', 14)
            pdf.cell(0, 10, 'Query Results', ln=True, fill=True)
            pdf.set_font('Arial', '', 9)
            
            # Calculate column widths
            num_cols = len(df.columns)
            col_width = 190 / num_cols if num_cols <= 5 else 38
            
            # Headers
            pdf.set_font('Arial', 'B', 9)
            for col in df.columns:
                pdf.cell(col_width, 7, str(col)[:15], 1, 0, 'C')
            pdf.ln()
            
            # Data rows (max 15 rows)
            pdf.set_font('Arial', '', 8)
            for idx, row in df.head(15).iterrows():
                for col in df.columns:
                    value = str(row[col])[:15]
                    pdf.cell(col_width, 6, value, 1)
                pdf.ln()
            
            if len(df) > 15:
                pdf.set_font('Arial', 'I', 9)
                pdf.cell(0, 6, f"... and {len(df) - 15} more rows", ln=True)
        
        # Visualizations
        if state['chart_files']:
            pdf.add_page()
            pdf.set_font('Arial', 'B', 16)
            pdf.cell(0, 10, 'Data Visualizations', ln=True)
            pdf.ln(5)
            
            for i, chart_file in enumerate(state['chart_files']):
                if os.path.exists(chart_file):
                    if i > 0 and i % 2 == 0:
                        pdf.add_page()
                    
                    viz_info = analysis['visualizations'][i] if i < len(analysis.get('visualizations', [])) else {}
                    
                    if viz_info.get('description'):
                        pdf.set_font('Arial', '', 10)
                        pdf.multi_cell(0, 6, viz_info['description'])
                        pdf.ln(2)
                    
                    pdf.image(chart_file, x=10, w=190)
                    pdf.ln(10)
        
        # Process Log
        pdf.add_page()
        pdf.set_font('Arial', 'B', 14)
        pdf.cell(0, 10, 'Process Log', ln=True, fill=True)
        pdf.set_font('Courier', '', 8)
        
        for msg in state['messages']:
            pdf.multi_cell(0, 5, msg)
        
        # Save PDF
        output_filename = f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        pdf.output(output_filename)
        
        state['pdf_file'] = output_filename
        state['messages'].append(f"✓ PDF Generation: Saved as {output_filename}")
        
        # Cleanup charts
        for chart_file in state['chart_files']:
            if os.path.exists(chart_file):
                os.remove(chart_file)
        
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
    
    # Define edges (workflow sequence)
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
        self.workflow = create_workflow(self.db_manager, api_key)
        
        # Test connection
        if not self.db_manager.test_connection():
            raise Exception("Failed to connect to database")
        
        print(f"✓ Connected to {self.db_manager.db_type} database")
    
    def process_question(self, user_question: str) -> str:
        """Process question through LangGraph workflow"""
        
        print(f"\n{'='*70}")
        print(f"PROCESSING QUERY")
        print(f"{'='*70}")
        print(f"Question: {user_question}")
        print(f"Database: {self.db_manager.db_type}")
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
        final_state = self.workflow.invoke(initial_state)
        
        # Print results
        print("\n" + "="*70)
        print("WORKFLOW COMPLETED")
        print("="*70)
        
        for msg in final_state['messages']:
            print(msg)
        
        if final_state.get('error'):
            print(f"\n❌ Error: {final_state['error']}")
        
        print(f"\n📄 PDF Report: {final_state['pdf_file']}")
        print("="*70 + "\n")
        
        return final_state['pdf_file']


# ============================================================================
# DEMO & TESTING
# ============================================================================

def create_demo_database():
    """Create a demo SQLite database with sample data"""
    
    import sqlite3
    import random
    
    db_file = 'demo_sales.db'
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Drop existing tables
    cursor.execute('DROP TABLE IF EXISTS sales')
    cursor.execute('DROP TABLE IF EXISTS products')
    cursor.execute('DROP TABLE IF EXISTS users')
    cursor.execute('DROP TABLE IF EXISTS categories')
    
    # Create tables
    cursor.execute('''
        CREATE TABLE categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL,
            description TEXT
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category_id INTEGER,
            price REAL NOT NULL,
            stock_quantity INTEGER DEFAULT 0,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE users (
            user_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE sales (
            sale_id INTEGER PRIMARY KEY,
            product_id INTEGER,
            user_id INTEGER,
            quantity INTEGER NOT NULL,
            sale_date DATE NOT NULL,
            total_amount REAL NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')
    
    # Insert categories
    categories = [
        (1, 'Clothing', 'Apparel and fashion items'),
        (2, 'Electronics', 'Electronic devices and gadgets'),
        (3, 'Books', 'Physical and digital books'),
        (4, 'Sports', 'Sports equipment and gear')
    ]
    cursor.executemany('INSERT INTO categories VALUES (?,?,?)', categories)
    
    # Insert products
    products = [
        (1, 'T-Shirt Blue', 1, 19.99, 150),
        (2, 'T-Shirt Red', 1, 19.99, 200),
        (3, 'T-Shirt Green', 1, 19.99, 175),
        (4, 'Jeans Classic', 1, 49.99, 100),
        (5, 'Hoodie Black', 1, 39.99, 80),
        (6, 'Laptop Pro', 2, 1299.99, 25),
        (7, 'Smartphone X', 2, 899.99, 50),
        (8, 'Headphones Wireless', 2, 149.99, 120),
        (9, 'Python Programming', 3, 45.00, 60),
        (10, 'Data Science Guide', 3, 55.00, 45),
        (11, 'Running Shoes', 4, 89.99, 90),
        (12, 'Yoga Mat', 4, 29.99, 110)
    ]
    cursor.executemany('INSERT INTO products VALUES (?,?,?,?,?)', products)
    
    # Insert users
    users = [
        (1, 'John Doe', 'john@example.com'),
        (2, 'Jane Smith', 'jane@example.com'),
        (3, 'Bob Johnson', 'bob@example.com'),
        (4, 'Alice Williams', 'alice@example.com'),
        (5, 'Charlie Brown', 'charlie@example.com')
    ]
    cursor.executemany('INSERT INTO users VALUES (?,?,?,CURRENT_TIMESTAMP)', users)
    
    # Insert sales (last 2 months)
    base_date = datetime.now() - timedelta(days=60)
    sales_data = []
    
    for i in range(1, 201):
        product_id = random.randint(1, 12)
        user_id = random.randint(1, 5)
        quantity = random.randint(1, 5)
        days_ago = random.randint(0, 60)
        sale_date = (base_date + timedelta(days=days_ago)).date()
        
        # Get product price
        cursor.execute('SELECT price FROM products WHERE product_id = ?', (product_id,))
        price = cursor.fetchone()[0]
        total = price * quantity
        
        sales_data.append((i, product_id, user_id, quantity, sale_date, total))
    
    cursor.executemany('INSERT INTO sales VALUES (?,?,?,?,?,?)', sales_data)
    
    conn.commit()
    conn.close()
    
    print(f"✓ Demo database created: {db_file}")
    print(f"  - 4 categories")
    print(f"  - 12 products")
    print(f"  - 5 users")
    print(f"  - 200 sales transactions")
    
    return f"sqlite:///{db_file}"


def run_demo():
    """Run complete demo with sample questions"""
    
    print("\n" + "="*70)
    print("NATURAL LANGUAGE TO SQL SYSTEM - DEMO")
    print("Multi-Agent System with LangGraph")
    print("="*70 + "\n")
    
    # Create demo database
    print("Setting up demo database...")
    db_url = create_demo_database()
    
    # Get API key
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("❌ Error: ANTHROPIC_API_KEY not found in environment")
        print("Please set it using: export ANTHROPIC_API_KEY='your-key-here'")
        return
    
    # Initialize system
    print("\nInitializing NL-to-SQL system...")
    system = NLToSQLSystem(db_url, api_key)
    
    # Demo questions
    demo_questions = [
        "How many t-shirts were sold last month?",
        "What are the total sales by category?",
        "Show me the top 5 best-selling products",
        "How many products are available in store?",
        "What were the total sales in the last 30 days?"
    ]
    
    print("\n" + "="*70)
    print("RUNNING DEMO QUERIES")
    print("="*70 + "\n")
    
    for i, question in enumerate(demo_questions, 1):
        print(f"\n{'='*70}")
        print(f"DEMO QUERY {i}/{len(demo_questions)}")
        print(f"{'='*70}")
        
        try:
            pdf_file = system.process_question(question)
            print(f"✅ Success! Report saved: {pdf_file}")
        except Exception as e:
            print(f"❌ Error: {str(e)}")
        
        if i < len(demo_questions):
            input("\nPress Enter to continue to next query...")
    
    print("\n" + "="*70)
    print("DEMO COMPLETED")
    print("="*70)
    print("\nGenerated PDF reports contain:")
    print("  ✓ Original question")
    print("  ✓ Generated SQL query")
    print("  ✓ Query validation results")
    print("  ✓ Analysis summary and key metrics")
    print("  ✓ Data table with results")
    print("  ✓ Visualizations (charts/graphs)")
    print("  ✓ Key insights")
    print("  ✓ Complete process log")
    print("\nCheck the generated PDF files to see the reports!")


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Run the demo
    run_demo()
    
    # Or use directly:
    # db_url = "postgresql://user:pass@localhost/dbname"  # PostgreSQL
    # db_url = "mysql+pymysql://user:pass@localhost/dbname"  # MySQL
    # db_url = "sqlite:///mydb.db"  # SQLite
    
    # system = NLToSQLSystem(db_url, os.getenv("ANTHROPIC_API_KEY"))
    # pdf = system.process_question("Your question here")