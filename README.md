# 📊 GQL-to-CSV Query Engine
> **A lightweight tool to query, filter, and join CSV files using a custom GraphQL-like syntax.**

## 🌟 Introduction
This engine treats a folder of CSV files like a relational database. It handles complex relationships (like a Director having many Movies) and produces clean, nested JSON output without writing SQL.

---

## 🛠️ Project Setup

1. Folder Structure:
   - project/
     - main.py                (Main runner)
    # 📊 GQL-to-CSV Query Engine

    > A lightweight tool to query, filter, and join CSV files using a custom GraphQL-like syntax.

    ## 🌟 Introduction

    This engine treats a folder of CSV files like a relational database. It handles complex relationships (like a Director having many Movies) and produces clean, nested JSON output without writing SQL.

    ---

    ## 🛠️ Project Setup

    1. Folder Structure:
         - d:/GQL/
             - `main.py`                (Main runner)
             - `generator.py`           (Query parser)
             - `Sample Queries/basic.gql` / `Sample Queries/auto-aliasing.gql` (Example queries)
             - `adapters/`
                 - `csv_adapter.py`       (CSV logic)
             - `data/`                  (Your .csv files)

    2. Dependencies:
         - Python 3.8+
         - pandas (for the CSV adapter):

    ```bash
    pip install pandas
    ```

    ---

    ## ✍️ The Query Syntax Guide (Quick)

    Symbols and meaning:

    - `<...>`: Table — selects a CSV file by name (no extension). Example: `<movies>`
    - `!`: Strict — hides parent if sub-table is empty
    - `~`: Internal — use for logic/filters, omit in output
    - `*`: Pluck — return raw list(s) of values
    - `:=`: Alias — save or rename fields
    - `$`: Global — define top-level macros inside `$global { ... }`

    ## 🚀 How to Use

    1. Define globals in your `.gql` file (optional):

    ```gql
    $global {
            min_budget : 50
            low_movie_budget : [$min_budget, 100]
            high_movie_budget : [101, 500]
            target_country : {"India", "Sri Lanka"}
    }
    ```

    2. Write your query using `<table>` blocks and fields. Example:

    ```gql
    <directors> {
            id := dir_var,
            country : $target_country,
            name,
            <movies> {
                    name,
                    ~budget : $low_movie_budget,
                    ~director_id = dir_var := directors_id
            } := low_budget_movies
    } := directors_india
    ```

    3. Run the tool:

    ```bash
    python main.py
    ```

    ---

    ## Overview

    - Purpose: Convert a compact GraphQL-like query language (GQL) into nested JSON by querying CSV files as tables.
    - Primary files: `main.py` (runner), `generator.py` (parser and macro resolver), `adapters/csv_adapter.py` (CSV execution).

    ## Repository Layout

    - `main.py`: Entrypoint; loads a `.gql` query file, invokes `GQLParser`, then calls the configured adapter to execute queries and produce JSON.
    - `generator.py`: Lexer, parser, macro resolver and expression evaluator. Produces an AST with `__meta__` information and resolved range/list values.
    - `adapters/`: Adapter implementations. This project ships a simple CSV adapter:
        - `csv_adapter.py` — reads CSV files from `data/` and applies filters.
        - `base_adapter.py` — adapter interface used for future adapters.

    Note: legacy adapters (file/SQL) were removed to keep the repo focused on the CSV adapter. If you need a SQL or file adapter, create one implementing the `BaseAdapter` interface.
    - `data/`: CSV files used as tables (e.g., `directors.csv`, `movies.csv`).

    ## How the Parser Works (High Level)

    - Tokenization: the parser tokenizes the query into logical tokens (IDs, TABLEs, STRINGs, NUMBERs, operators, braces, parentheses, etc.).
    - Parsing: builds a nested AST matching GQL nesting and annotations. Each table node gets a `__meta__` object with keys like `table_source`, `alias`, `pluck`, `strict_keys`, and `internal_keys`.
    - Global macros (`$global`): collected first. Raw macro values are read as strings and then resolved in two passes so macros can reference other macros.
    - Macro resolution & evaluation: after substitution, macros are evaluated safely using a small AST-based evaluator. Ranges like `[min,max]`, lists like `{"a","b"}`, numeric expressions, and bitwise operations are supported.
    - Prints in globals: `print(...)` calls inside `$global` are executed at macro-resolution time for quick debugging; they print evaluated values.

    ## Supported Operators and Expressions

    - Arithmetic: `+`, `-`, `*`, `/`, `//`, `%`, `**`
    - Bitwise: `<<`, `>>`, `&`, `|`, `^`, `~` (unary invert)
    - Unary: `+`, `-`

    Expression evaluation is implemented using Python's `ast` module with a whitelist of allowed AST node types and operators — no arbitrary code execution.

    ## GQL Language Reference (Details)

    - `<table>`: table node; matches a CSV filename in `data/` (without extension).
    - `:` (colon): field mapping to a value or expression. Example: `country : $target_country`.
    - `:=`: alias/save field to a variable. Example: `id := director_id`.
    - `!`: strict table marker (hides parent when subtable empty).
    - `~`: internal field — used in filters/logic but omitted from final JSON output.
    - `*`: pluck — returns raw list of values for the selected fields.
    - `$global { ... }`: global variables block. Declarations may be expressions, ranges, lists, strings, or nested macro references.

    Examples:

    - Range: `low_movie_budget : [$min_budget, $min_budget * 2]` → injected as `{"__ge__": value, "__le__": value}` used by adapters to filter numeric columns.
    - List: `target_country : {"India", "USA"}` → injected as Python list `['India', 'USA']` for IN-style filters.

    ## Adapter Interface

    - Each adapter exposes an `execute(table_source, node)` function used by `main.py`.
    - `table_source` is the original table name (from `<...>`). The parser may rename AST keys to avoid collisions; the adapter should use `node['__meta__']['table_source']` to find the correct CSV file.
    - The CSV adapter expects CSV files to have headers matching the column names used in the query.

    ## AST Shape

    Top-level AST is a dict where each key is either an alias or a generated unique name for repeated un-aliased tables (format: `<table>_<index>`). Each node is a dict containing:

    - `__meta__`: metadata including `table_source`, `alias`, `pluck`, `strict_keys`, `internal_keys`.
    - fields: either nested nodes or filter objects (ranges converted into `__ge__/__le__` keys).

    Example snippet:

    ```json
    {
        "directors_math_1": {
            "__meta__": {"table_source": "directors", "alias": "directors_math_1"},
            "country": ["India", "USA"],
            "low_budget_movies": {"__meta__": {"table_source":"movies"}, "budget": {"__ge__": 50, "__le__": 100}}
        }
    }
    ```

    ## Testing & Debugging

    - You can add unit tests that call `GQLParser(verbose=True).parse(query_string)` and assert resolved macro values and AST shape.
    - Use `print(...)` inside `$global` while authoring queries — those prints are executed during macro resolution and help inspect evaluated macro values.

    ## Common Troubleshooting

    - Variable not found: ensure global variables are declared with a leading `$` inside `$global` and referenced with the same `$name` syntax.
    - Duplicate table names: the parser auto-renames repeated un-aliased tables to `table_0`, `table_1`, ... to avoid AST key collisions; use `:=` aliases to set explicit keys.
    - Quotes in braced lists: the parser normalizes braced lists like `{India, \"USA\"}` so string items are properly quoted; prefer explicit quoting.

    ## Development Notes

    - The expression evaluator uses `ast.parse(..., mode='eval')` and traverses allowed AST nodes only. Be cautious adding new node types.
    - Tokenization order matters: `TABLE` tokens (`<name>`) must be recognized before generic operator tokens to avoid conflicts.

    ## Contributing

    - Add feature branches, run the main pipeline, and include unit tests for parser behavior you change.

    ## Run Examples

    Run the main example with the included `Sample Queries/basic.gql` or `Sample Queries/auto-aliasing.gql`:

    ```bash
    python main.py
    ```

    That will print any `print(...)` calls from `$global`, show verbose macro resolution logs if enabled, and output the final JSON result.

    ---

    If you'd like, I can also add a short `DEVELOPMENT.md` describing how to run parser-only tests and how to extend adapters.
    - Quotes in braced lists: The parser normalizes braced lists like `{India, "USA"}` so string items are properly quoted; prefer explicit quoting.
