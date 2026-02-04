# 🤖 Natural Language to SQL System

**Production-ready AI-powered system that converts natural language questions into SQL queries, executes them, and generates comprehensive PDF reports with visualizations.**

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![LangGraph](https://img.shields.io/badge/LangGraph-Multi--Agent-green.svg)](https://github.com/langchain-ai/langgraph)
[![Anthropic Claude](https://img.shields.io/badge/Anthropic-Claude%204-orange.svg)](https://www.anthropic.com/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

---

## 🌟 Features

### 🎯 Core Capabilities
- **Natural Language Processing**: Ask questions in plain English, get SQL queries automatically
- **Multi-Database Support**: Works with PostgreSQL, MySQL, and SQLite
- **Multi-Agent Architecture**: Uses LangGraph for intelligent workflow orchestration
- **Query Validation**: Security and performance checks before execution
- **Professional Reports**: Auto-generated PDF reports with charts and insights
- **Production-Ready**: Optimized for performance, security, and reliability

### 🤖 AI Agents
1. **Schema Analysis Agent** - Identifies relevant tables and relationships
2. **SQL Generation Agent** - Creates optimized, production-ready SQL queries
3. **Query Validation Agent** - Validates for security, correctness, and performance
4. **Query Execution Agent** - Safely executes validated queries
5. **Data Analysis Agent** - Generates executive-level insights
6. **Visualization Agent** - Creates professional charts and graphs
7. **PDF Generation Agent** - Produces comprehensive PDF reports

---

## 📊 Sample Output

### Input
```
Question: "Show me the top 5 best-selling products"
```

### Output
- ✅ **SQL Query**: Automatically generated and validated
- ✅ **Data Results**: Query execution with formatted tables
- ✅ **Visualizations**: Professional bar charts, pie charts, line graphs
- ✅ **PDF Report**: Complete analysis with insights and recommendations
- ✅ **Process Log**: Detailed workflow tracking

---

## 🚀 Quick Start

### Prerequisites
- Python 3.8 or higher
- Anthropic API key ([Get one here](https://console.anthropic.com/))

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/nl-to-sql-system.git
cd nl-to-sql-system
```

2. **Create virtual environment**
```bash
python -m venv venv

# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. **Install dependencies**
```bash
pip install -r Requirements.txt
```

4. **Set up environment variables**

Create a `.env` file in the project root:
```env
ANTHROPIC_API_KEY=your_api_key_here
```

5. **Create demo database** (optional)
```bash
python demo_database.py
```

6. **Run the system**
```bash
python nl_to_sql_langgraph.py
```

---

## 📁 Project Structure

```
nl-to-sql-system/
├── nl_to_sql_langgraph.py    # Main application (Multi-Agent System)
├── demo_database.py           # Demo database generator
├── Requirements.txt           # Python dependencies
├── .env                       # Environment variables (create this)
└── README.md                 # This file
```

---

## 🎮 Usage

### Interactive Mod

Run the system and select from demo questions:

```bash
python nl_to_sql_langgraph.py
```

**Demo Questions:**
1. How many t-shirts were sold last month?
2. What are the total sales by category?
3. Show me the top 5 best-selling products
4. How many products are available in the store?
5. What were the total sales in the last 30 days?
6. Which users made the most purchases?
7. What is the average order value?
8. Show sales trends over the last 3 months
9. Custom question

### Custom Database

To use your own database, modify the connection string:

```python
# PostgreSQL
db_url = "postgresql://user:password@localhost:5432/database"

# MySQL
db_url = "mysql+pymysql://user:password@localhost:3306/database"

# SQLite
db_url = "sqlite:///your_database.db"
```

---

## 📦 Dependencies

### Core
- **langgraph** - Multi-agent workflow orchestration
- **anthropic** - Claude AI integration
- **sqlalchemy** - Database abstraction layer
- **pandas** - Data manipulation and analysis
- **python-dotenv** - Environment variable management

### Visualization
- **matplotlib** - Chart generation
- **seaborn** - Statistical visualizations

### PDF Generation
- **fpdf2** - PDF report creation
- **Pillow** - Image processing

### Optional Database Drivers
- **psycopg2-binary** - PostgreSQL support
- **pymysql** - MySQL support

---

## 🔒 Security Features

- ✅ SQL injection prevention
- ✅ Dangerous operation blocking (DROP, DELETE, TRUNCATE)
- ✅ Query validation before execution
- ✅ Read-only query enforcement
- ✅ Schema-based validation

---

## 📈 Performance Optimizations

### PDF Reports (70% Smaller)
- **Chart DPI**: 150 (optimized from 300)
- **Figure sizes**: Compact, efficient layouts
- **Table display**: Maximum 8 columns, 20 rows
- **File size**: 1-3 MB (previously 5-10 MB)

### Query Optimization
- Minimal table selection
- Proper JOIN conditions
- Efficient WHERE clauses
- Result limiting for large datasets

---

## 🎨 PDF Report Sections

1. **User Question** - Original natural language query
2. **Generated SQL Query** - Formatted SQL with syntax highlighting
3. **Query Validation** - Security and correctness checks
4. **Analysis Summary** - Executive-level insights
5. **Key Metrics** - Important numbers with context
6. **Query Results** - Professional table with alternating rows
7. **Data Visualizations** - Charts and graphs
8. **Process Log** - Detailed workflow tracking

---

## 🛠️ Configuration

### Database Types

The system auto-detects database type from the connection string:

```python
SUPPORTED_DBS = {
    'postgresql': 'postgresql://',
    'mysql': 'mysql+pymysql://',
    'sqlite': 'sqlite:///'
}
```

### Visualization Settings

Customize chart appearance in `VisualizationAgent`:

```python
plt.rcParams['font.size'] = 10
plt.rcParams['axes.titlesize'] = 12
plt.rcParams['axes.labelsize'] = 10
```

### PDF Settings

Adjust PDF layout in `PDFGenerationAgent`:

```python
MIN_COL_WIDTH = 30  # Minimum column width (mm)
MAX_COLS = 8        # Maximum columns to display
MAX_ROWS = 20       # Maximum rows to display
```

---

## 🧪 Testing

### Test with Demo Database

```bash
# Create demo database
python demo_database.py

# Run system
python nl_to_sql_langgraph.py
```

### Test with Custom Questions

Select option 9 (Custom question) and enter your own queries.

---

## 📊 Example Workflow

```mermaid
graph TD
    A[User Question] --> B[Schema Analysis Agent]
    B --> C[SQL Generation Agent]
    C --> D[Query Validation Agent]
    D --> E{Safe to Execute?}
    E -->|Yes| F[Query Execution Agent]
    E -->|No| G[Error Report]
    F --> H[Data Analysis Agent]
    H --> I[Visualization Agent]
    I --> J[PDF Generation Agent]
    J --> K[PDF Report]
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- **Anthropic** - Claude AI API
- **LangGraph** - Multi-agent framework
- **SQLAlchemy** - Database toolkit
- **Matplotlib & Seaborn** - Visualization libraries
- **FPDF2** - PDF generation

---

## 📧 Support

For issues, questions, or suggestions:
- Open an issue on GitHub
- Contact: your.email@example.com

---

## 🔮 Future Enhancements

- [ ] Support for more database types (Oracle, MS SQL Server)
- [ ] Web interface with Flask/FastAPI
- [ ] Real-time query streaming
- [ ] Query history and caching
- [ ] Multi-language support
- [ ] Advanced chart types (scatter, heatmap, etc.)
- [ ] Export to Excel, CSV, JSON
- [ ] Scheduled report generation
- [ ] User authentication and permissions

---

## 📚 Documentation

- [ENHANCEMENTS.md](ENHANCEMENTS.md) - Detailed enhancement documentation
- [API Documentation](#) - Coming soon
- [User Guide](#) - Coming soon

---

**Made with ❤️ using Claude AI and LangGraph**
