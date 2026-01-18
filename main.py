import json
import os
from generator import GQLParser
from adapters.csv_adapter import CSVAdapter

def run():
    # 1. Read the Query file
    query_file = "query.gql"
    if not os.path.exists(query_file):
        print(f"Error: {query_file} not found.")
        return

    with open(query_file, "r") as f:
        gql = f.read()

    # 2. Parse the Query into an AST
    parser = GQLParser(verbose=True)
    try:
        query_ast = parser.parse(gql) 
    except Exception as e:
        print(f"Parsing Error: {e}")
        return

    print("\n--- FULLY RESOLVED AST ---")
    print(json.dumps(query_ast, indent=4))
    print("-" * 30)

    # 3. Initialize Adapter
    # Ensure the 'data' folder exists and contains your CSVs
    adapter = CSVAdapter(folder_path="./data", verbose=True)
    
    # 4. Execute and Handle Aliasing
    final_output = {}
    
    for table_name, node in query_ast.items():
        # The key in query_ast is the CSV filename (e.g., 'directors')
        # We pass this to the adapter so it finds 'directors.csv'
        results = adapter.execute(table_name, node)
        
        # Check if a top-level alias was defined (e.g., <directors> { ... } := details)
        # If no alias exists, fallback to the table_name
        json_key = node.get("__meta__", {}).get("alias", table_name)
        
        final_output[json_key] = results
    
    # 5. Output Final JSON
    print("\n--- FINAL JSON RESULTS ---")
    print(json.dumps(final_output, indent=4))

if __name__ == "__main__":
    run()