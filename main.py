import json
from generator import ql_to_json
# Assuming your adapter is in adapters/csv_adapter.py
from adapters.csv_adapter import CSVAdapter 

def run_query(gql_file_path, data_folder):
    with open(gql_file_path, 'r') as f:
        gql_content = f.read()

    # Get AST as a dictionary
    query_ast = ql_to_json(gql_content) 
    # print(json.dumps(query_ast, indent=4))

    # Initialize Adapter with folder where CSVs are kept
    adapter = CSVAdapter(folder_path=data_folder)

    # Execute from the root table
    root_table = list(query_ast.keys())[0]
    results = adapter.execute(root_table, query_ast[root_table])
    
    print(json.dumps(results, indent=4))

if __name__ == "__main__":
    # Point to the directory containing directors.csv and movies.csv
    run_query("query.gql", "./data")