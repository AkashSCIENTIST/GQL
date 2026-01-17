# GQL-JSON Query Engine (Advanced)

A highly recursive, metadata-driven query engine that translates custom Graph Query Language (GQL) into a structured JSON AST for processing across relational CSV data sources.

## 🚀 Key Features

### 1. Multi-Root Queries & Global Variables
Supports querying multiple disparate tables in a single GQL file and defining a `variables {}` block for constants used throughout the query.

### 2. Nested Relational Joins
Perform deep lookups (e.g., Directors → Movies → Actors) by passing parent-scoped variables into child filters using the '=' operator.

### 3. Metadata Operators
| Operator | Name | Function |
| :--- | :--- | :--- |
| **`!`** | **Strict** | Discards parent row if the child block returns no results (Inner Join). |
| **`~`** | **Internal** | Field is used for filtering or logic but is stripped from final JSON. |
| **`*`** | **Pluck** | If a block results in one visible field, it flattens the list of objects into a simple array. |

### 4. Arithmetic Ranges & Sets
Full support for mathematical interval notation and set-based filtering:
- **Inclusive**: [50, 100] (>= 50 AND <= 100)
- **Exclusive**: (0, 10) (> 0 AND < 10)
- **Half-Open**: [50, 100) or (0, 10]
- **Sets**: {"India", "USA"} (Membership check)

### 5. Advanced Aliasing
Rename columns or entire nested result blocks using the ':=' operator.
- Column: id := dir_var
- Block: movies { ... } := directed_movies

---

## 🛠 Syntax Reference

### Variable Definition & Conditional Nesting
Example of using global variables to filter nested blocks:

variables {
    min_budget : 50,
    target_country : "India"
}

directors {
    name,
    country : target_country,  # Using global variable
    !movies *{
        name,
        ~budget : [min_budget, 500],
        ~director_id = id
    } := high_budget_films
}

---

## 💻 Technical Implementation

### Phase 1: generator.py (The Parser)
A Recursive Descent Parser with depth-aware expression handling. It tracks brace depth to ensure complex ranges like [10, 20] are captured as single tokens.



### Phase 2: csv_adapter.py (The Engine)
Built on Pandas, the adapter performs:
1. **Dynamic Reindexing**: Prevents UserWarnings by aligning boolean masks with current DataFrame states.
2. **Context Propagation**: Maintains a dictionary of variables (parent keys and globals) passed down the recursion tree.
3. **Array Flattening**: Detects the 'pluck' flag to simplify single-field outputs.

---

## 📊 Example Execution

### Input GQL:
directors {
    id := d_id,
    movies *{
        name,
        ~budget : [50, 100],
        ~director_id = d_id
    } := titles
}

### Output JSON:
[
    {
        "d_id": "d1",
        "titles": ["Movie A", "Movie B"]
    }
]

---

## 📄 License
MIT License