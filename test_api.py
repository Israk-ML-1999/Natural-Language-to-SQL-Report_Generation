# Test API Endpoint
# Save this as test_api.py and run: python test_api.py

import requests
import json

# Test health endpoint
try:
    response = requests.get('http://localhost:8089/health')
    print(f"✅ Health Check: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
except Exception as e:
    print(f"❌ Health Check Failed: {e}")

print("\n" + "="*50 + "\n")

# Test analyze endpoint
try:
    data = {
        "question": "How many t-shirts were sold last month?",
        "database": "sqlite:///demo_sales.db"
    }
    
    response = requests.post(
        'http://localhost:8089/api/analyze',
        json=data,
        headers={'Content-Type': 'application/json'}
    )
    
    print(f"✅ Analyze API: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    
except Exception as e:
    print(f"❌ Analyze API Failed: {e}")
