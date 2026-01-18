# 📊 GQL-to-CSV Query Engine
> **A lightweight tool to query, filter, and join CSV files using a custom GraphQL-like syntax.**

## 🌟 Introduction
This engine treats a folder of CSV files like a relational database. It handles complex relationships (like a Director having many Movies) and produces clean, nested JSON output without writing SQL.

---

## 🛠️ Project Setup

1. Folder Structure:
   - project/
     - main.py                (Main runner)
     - generator.py           (Query parser)
     - query.gql              (Your GQL query)
     - adapters/
       - csv_adapter.py       (CSV logic)
     - data/                  (Your .csv files)

2. Dependencies:
   Requires Pandas.
   - pip install pandas

---

## ✍️ The Query Syntax Guide

| Symbol | Name     | Description                                  | Example           |
| :----- | :------- | :------------------------------------------- | :---------------- |
| <...>  | Table    | Defines which CSV file to open.              | <movies>          |
| !      | Strict   | Hides parent if sub-table is empty.          | !<movies>         |
| ~      | Internal | Uses field for logic but hides it from JSON. | ~budget           |
| * | Pluck    | Returns only raw values (removes keys).      | *{ name }         |
| :=     | Alias    | Renames a field or saves to a variable.      | id := dir_var     |
| \$      | Global   | Defines reusable variables at the top.       | $min_budget       |

---

## 🚀 How to Use

### 1. Define Globals (query.gql)
$global {
    target: "India",
    min_pay: 50
}

### 2. Write your Query (query.gql)
```
$global {
    min_budget : 50,
    low_movie_budget : [$min_budget, 100],
    high_movie_budget : [101, 500],
    target_country : {"India", "Sri Lanka"}
}

<directors> {
    id := dir_var,
    country : $target_country,  # Using global variable
    name,
    <movies> {
        name,
        ~budget : $low_movie_budget,
        ~director_id = dir_var := directors_id
    } := low_budget_movies,
    <movies> {
        name,
        budget : $high_movie_budget,
        ~director_id = dir_var := directors_id
    } := high_budget_movies
} := directors_india

<directors> {
    id := dir_var,
    country : "USA",  # Using local variable
    name,
    <movies> {
        name,
        ~budget : $low_movie_budget,
        ~director_id = dir_var := directors_id
    } := low_budget_movies,
    <movies> {
        name,
        budget : $high_movie_budget,
        ~director_id = dir_var := directors_id
    } := high_budget_movies
} := directors_usa
```

### 3. Run it
- python main.py

---

## 🔍 Understanding "Pluck" Output (*)

The pluck symbol makes JSON cleaner:
- Single Field: movies *{ name } -> ["Singam", "Avatar"]
- Multi Field: movies *{ name, genre } -> ["Singam", "Action"]

---

## 🛠️ Troubleshooting

- Empty list []: Check ! marks. If filters are too tight, strict tables hide everything. Ensure CSVs have no leading spaces.
- Variable not found: Ensure variables in $global start with $.
- Column not found: Match query names to CSV header names exactly.

## ⚙️ Features to be added

- Strict key support when multiple subtables are used
