# GQL-JSON Query Engine (Python)

A specialized, lightweight query engine that parses a custom Graph Query Language (GQL) into a JSON-based Abstract Syntax Tree (AST) and executes recursive data retrieval across CSV files.

## 🚀 Key Features

- **Recursive Joins**: Perform nested lookups (e.g., fetching 'movies' for each 'director') using parent-scoped variables.
- **Advanced Interval Filtering**: Support for all mathematical range notations:
    - Inclusive: [x, y] (>= x, <= y)
    - Exclusive: (x, y) (> x, < y)
    - Half-open: [x, y) or (x, y]
- **Strict Child Requirements (!)**: If a nested table is prefixed with '!', the parent row is discarded if no children match (Logical INNER JOIN behavior).
- **Internal Fields (~)**: Prefix fields with '~' to use them for logic, variable assignment, or filtering without including them in the final JSON output.
- **Dual-Type Matching**: Automatically handles numeric and string comparisons to prevent data loss in unformatted CSVs.

---

## 🛠 Project Architecture



### 1. generator.py
The heart of the engine. It uses a Recursive Descent Parser to handle complex line expressions like 'director_id = dir_var := directors_id'. It distinguishes between data sets {} and nested table blocks using lookahead logic.

### 2. csv_adapter.py
The execution layer built on Pandas. It performs row pruning and context propagation, ensuring that variables assigned in parent levels are available for child filters. It includes index-alignment logic to prevent Pandas UserWarnings.

---

## 📝 Syntax Reference

### Variable Assignment & Aliasing
Use ':=' to rename a field or save it to the context.
Example: id := dir_var  # 'id' column appears as 'dir_var' in JSON

### Relational Matching
Use '=' to filter a child table based on a variable captured from a parent.
Example: ~director_id = dir_var  # Filter child rows where director_id matches parent

### Complex Filtering
Example: budget : [50, 150)  # Budget >= 50 AND < 150
Example: country : {"India"} # Set-based membership

---

## 💻 Usage Example

import json
from generator import ql_to_json
from adapters.csv_adapter import CSVAdapter

# 1. Define your GQL Query
gql = """
directors {
    id := dir_var,
    name,
    country : {"India"},
    !movies {
        name,
        budget : [50, 150),
        ~director_id = dir_var := directors_id
    }
}
"""

# 2. Parse and Execute
query_ast = ql_to_json(gql)
adapter = CSVAdapter(folder_path="./data")

root_table = list(query_ast.keys())[0]
results = adapter.execute(root_table, query_ast[root_table])

print(json.dumps(results, indent=4))

---

## ⚙️ Data Requirements
The engine assumes a file-per-table structure. To query 'directors' and 'movies', ensure 'directors.csv' and 'movies.csv' exist in your data folder.

## 📄 License
MIT License