import os
import pathway as pw
from msal import ConfidentialClientApplication
import requests
import json

PORT = int(os.getenv("PORT", "8080"))

# Microsoft Graph credentials (must be set as environment variables)
TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")

# Get Microsoft Graph access token
def get_access_token():
    authority = f"https://login.microsoftonline.com/{TENANT_ID}"
    app = ConfidentialClientApplication(
        CLIENT_ID,
        authority=authority,
        client_credential=CLIENT_SECRET
    )
    result = app.acquire_token_for_client(scopes=["https://graph.microsoft.com/.default"])
    if "access_token" in result:
        return result["access_token"]
    else:
        print(f"Error acquiring token: {result.get('error_description')}")
        return None

# Fetch files from SharePoint/OneDrive
def fetch_o365_documents():
    token = get_access_token()
    if not token:
        return []
    
    headers = {"Authorization": f"Bearer {token}"}
    documents = []
    
    try:
        # Get all sites
        sites_url = "https://graph.microsoft.com/v1.0/sites?search=*"
        sites_response = requests.get(sites_url, headers=headers)
        sites = sites_response.json().get("value", [])
        
        print(f"Found {len(sites)} SharePoint sites")
        
        # For each site, get documents
        for site in sites[:5]:  # Limit to first 5 sites for now
            site_id = site["id"]
            site_name = site.get("displayName", "Unknown")
            
            # Get drive items from site
            drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drive/root/children"
            drive_response = requests.get(drive_url, headers=headers)
            
            if drive_response.status_code == 200:
                files = drive_response.json().get("value", [])
                
                for file in files[:10]:  # Limit to 10 files per site
                    if file.get("file"):  # Only process files, not folders
                        file_name = file.get("name", "")
                        # Only process text-based files
                        if any(file_name.endswith(ext) for ext in ['.txt', '.md', '.docx', '.pdf']):
                            documents.append({
                                "name": file_name,
                                "site": site_name,
                                "url": file.get("webUrl", ""),
                                "content": f"Document from {site_name}: {file_name}"
                            })
        
        print(f"Fetched {len(documents)} documents from Office 365")
        
    except Exception as e:
        print(f"Error fetching documents: {str(e)}")
    
    return documents

# Initialize documents
print("Connecting to Office 365...")
docs = fetch_o365_documents()

# Create schema for documents
class DocumentSchema(pw.Schema):
    name: str
    site: str
    url: str
    content: str

# Create Pathway table from documents
if docs:
    documents_table = pw.debug.table_from_rows(
        schema=DocumentSchema,
        rows=[(d["name"], d["site"], d["url"], d["content"]) for d in docs]
    )
else:
    # Create empty table if no documents
    documents_table = pw.debug.table_from_rows(
        schema=DocumentSchema,
        rows=[]
    )

# Set up REST API
class InputSchema(pw.Schema):
    query: str

webserver = pw.io.http.PathwayWebserver(
    host="0.0.0.0",
    port=PORT,
)

input_table, response_writer = pw.io.http.rest_connector(
    webserver=webserver,
    schema=InputSchema,
    delete_completed_queries=True,
)

# Simple search function
def search_documents(query):
    if not docs:
        return {"status": "No documents found", "count": 0, "query": query}
    
    # Simple keyword search (you'll enhance this with embeddings later)
    query_lower = query.lower()
    matching_docs = [
        {"name": d["name"], "site": d["site"], "url": d["url"]} 
        for d in docs 
        if query_lower in d["name"].lower() or query_lower in d["site"].lower()
    ]
    
    return {
        "status": "success",
        "query": query,
        "total_documents": len(docs),
        "matching_documents": len(matching_docs),
        "results": matching_docs[:5]  # Return top 5 matches
    }

# Process queries
output = input_table.select(
    query_id=input_table.id,
    result=pw.apply(lambda q: json.dumps(search_documents(q)), pw.this.query)
)

response_writer(output)

print(f"Starting Pathway webserver on port {PORT}...")
pw.run(monitoring_level=pw.MonitoringLevel.NONE)
