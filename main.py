import json
from generator import GQLParser
from adapters.csv_adapter import CSVAdapter

def run():
    with open("query.gql", "r") as f:
        gql = f.read()

    parser = GQLParser()
    query_ast = parser.parse(gql) 

    print("--- FULLY RESOLVED AST ---")
    print(json.dumps(query_ast, indent=4))
    print("-" * 30)

    # Note: Use the CSVAdapter provided in previous steps
    adapter = CSVAdapter(folder_path="./data")
    
    output = {}
    for table_name in query_ast:
        output[table_name] = adapter.execute(table_name, query_ast[table_name])
    
    print("--- RESULTS ---")
    print(json.dumps(output, indent=4))

if __name__ == "__main__":
    run()