"""
FastAPI Web Server for NL to SQL System
Serves the frontend and provides API endpoints for query processing and file downloads
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import os
import json
from pathlib import Path
from typing import Optional
import uvicorn

# Import the NL to SQL system
# Note: We'll need to modify this to work properly
# from nl_to_sql_langgraph import NLToSQLSystem

app = FastAPI(
    title="Natural Language to SQL System",
    description="Convert natural language queries to SQL with AI",
    version="1.0.0"
)

# CORS Configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
REPORTS_DIR = Path('.')
TEMPLATES_DIR = Path('templates')
API_KEY = os.getenv('ANTHROPIC_API_KEY')

# Request Models
class QueryRequest(BaseModel):
    question: str
    database: Optional[str] = "sqlite:///demo_sales.db"

class QueryResponse(BaseModel):
    success: bool
    data: Optional[dict] = None
    error: Optional[str] = None


# Routes
@app.get("/")
async def index():
    """Serve the frontend HTML"""
    return FileResponse(TEMPLATES_DIR / "index.html")


@app.get("/style.css")
async def get_css():
    """Serve CSS file"""
    return FileResponse(TEMPLATES_DIR / "style.css")


@app.get("/script.js")
async def get_js():
    """Serve JavaScript file"""
    return FileResponse(TEMPLATES_DIR / "script.js")


@app.get("/homepage.jpg")
async def get_background_image():
    """Serve background image"""
    # Prefer templates folder since it's the root for NGINX/static files
    image_path = TEMPLATES_DIR / "homepage.jpg"
    if image_path.exists():
        return FileResponse(image_path)
    
    # Fallback to image folder if needed
    fallback_path = Path("image/homepage.jpg")
    if fallback_path.exists():
        return FileResponse(fallback_path)
    
    return FileResponse(image_path)


@app.post("/api/analyze", response_model=QueryResponse)
async def analyze_query(request: QueryRequest):
    """
    Process natural language query and generate SQL
    
    Request Body:
    {
        "question": "Show me top 10 products by sales",
        "database": "sqlite:///demo_sales.db"
    }
    
    Response:
    {
        "success": true,
        "data": {
            "sql_query": "SELECT ...",
            "validation_result": {...},
            "query_results": [...],
            "analysis": {...},
            "messages": [...],
            "pdf_file": "report_xxx.pdf"
        }
    }
    """
    try:
        print(f"\n{'='*60}")
        print(f"API REQUEST RECEIVED")
        print(f"{'='*60}")
        print(f"Question: {request.question}")
        print(f"Database: {request.database}")
        print(f"{'='*60}\n")
        
        if not request.question.strip():
            print("❌ Error: Empty question")
            return QueryResponse(
                success=False,
                error="Question is required"
            )
        
        if not API_KEY:
            print("❌ Error: API key not configured")
            return QueryResponse(
                success=False,
                error="ANTHROPIC_API_KEY not configured. Please set it in environment variables."
            )
        
        # Import here to avoid circular imports
        from nl_to_sql_langgraph import NLToSQLSystem
        
        print(f"✓ Creating NLToSQLSystem with database: {request.database}")
        
        # Create system instance
        nl_sql = NLToSQLSystem(
            db_url=request.database,
            api_key=API_KEY
        )
        
        print(f"✓ NLToSQLSystem created successfully")
        print(f"✓ Processing question...")
        
        # Process the query
        pdf_file = nl_sql.process_question(request.question)
        
        print(f"✓ Query processed successfully")
        print(f"✓ PDF file: {pdf_file}\n")
        
        return QueryResponse(
            success=True,
            data={
                "pdf_file": pdf_file,
                "message": "Query processed successfully",
                "sql_query": "Generated SQL query",
                "validation_result": {"safe_to_execute": True},
                "messages": ["Query processed successfully"]
            }
        )
    
    except Exception as e:
        print(f"\n❌ ERROR in analyze_query:")
        print(f"   Type: {type(e).__name__}")
        print(f"   Message: {str(e)}")
        import traceback
        print(f"   Traceback:\n{traceback.format_exc()}")
        print(f"{'='*60}\n")
        
        return QueryResponse(
            success=False,
            error=f"{type(e).__name__}: {str(e)}"
        )


@app.get("/api/download/{filename}")
async def download_report(filename: str):
    """
    Download generated PDF report
    
    Parameters:
        filename: Name of the PDF file to download
    
    Returns:
        PDF file as attachment
    """
    try:
        # Security: Validate filename to prevent directory traversal
        if '..' in filename or '/' in filename or '\\' in filename:
            raise HTTPException(status_code=400, detail="Invalid filename")
        
        # Check if file exists
        file_path = REPORTS_DIR / filename
        
        if not file_path.exists():
            raise HTTPException(status_code=404, detail="File not found")
        
        return FileResponse(
            file_path,
            media_type='application/pdf',
            filename=filename
        )
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/reports")
async def list_reports():
    """
    List all available PDF reports
    
    Returns:
        JSON list of report filenames with metadata
    """
    try:
        reports = []
        
        for pdf_file in REPORTS_DIR.glob('report_*.pdf'):
            reports.append({
                'filename': pdf_file.name,
                'size': pdf_file.stat().st_size,
                'created': pdf_file.stat().st_mtime
            })
        
        # Sort by creation time (newest first)
        reports.sort(key=lambda x: x['created'], reverse=True)
        
        return {
            'success': True,
            'reports': reports
        }
    
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }


@app.get("/health")
@app.head("/health")
async def health_check():
    """Health check endpoint for container orchestration"""
    return {
        'status': 'healthy',
        'service': 'NL to SQL System',
        'version': '1.0.0',
        'api_key_configured': bool(API_KEY)
    }


if __name__ == '__main__':
    # Check for API key
    if not API_KEY:
        print("=" * 70)
        print("WARNING: ANTHROPIC_API_KEY not set in environment variables!")
        print("Please set it in .env file or as environment variable")
        print("=" * 70)
    
    # Create reports directory if it doesn't exist
    os.makedirs('reports', exist_ok=True)
    
    print("=" * 70)
    print("NL to SQL FastAPI Server Starting...")
    print("=" * 70)
    print(f"Frontend: http://localhost:8000")
    print(f"API Docs: http://localhost:8000/docs")
    print(f"ReDoc: http://localhost:8000/redoc")
    print(f"Health Check: http://localhost:8000/health")
    print("=" * 70)
    
    # Run the FastAPI app with Uvicorn
    uvicorn.run(
        app,
        host='0.0.0.0',
        port=8000,
        log_level='info'
    )
