import json
import pandas as pd # type: ignore
import os
from generator import ql_to_json

class CSVAdapter:
    def __init__(self, folder_path="."):
        self.folder_path = folder_path

    def _apply_filter(self, df, col, logic):
        """Processes __ge__, __le__, and $or operators."""
        if isinstance(logic, dict):
            for op, val in logic.items():
                if op == "__ge__": df = df[df[col].astype(float) >= float(val)]
                elif op == "__le__": df = df[df[col].astype(float) <= float(val)]
                elif op == "$or": df = df[df[col].astype(str).isin([str(i) for i in val])]
        return df

    def execute(self, table_name, node, context=None):
        file_path = os.path.join(self.folder_path, f"{table_name}.csv")
        if not os.path.exists(file_path): return []
        
        df = pd.read_csv(file_path)
        
        # 1. Row-level Filtering (Remains same as previous)
        for key, logic in node.items():
            if key in df.columns:
                if isinstance(logic, str) and logic.startswith("="):
                    var_name = logic[1:]; val = context.get(var_name) if context else None
                    if val is not None: df = df[df[key].astype(str) == str(val)]
                elif isinstance(logic, dict) and any(k.startswith(('_', '$')) for k in logic.keys()):
                    df = self._apply_filter(df, key, logic)

        # 2. Updated Result Construction
        results = []
        for _, row in df.iterrows():
            item = {}
            row_context = context.copy() if context else {}
            
            # Assignments first: id := dir_var
            for key, val in node.items():
                if isinstance(val, str) and val.startswith(":="):
                    alias_name = val[2:] # Extract 'dir_var'
                    row_context[alias_name] = row[key]
                    item[alias_name] = row[key] # Use 'dir_var' as the KEY
            
            # Selections and Nesting
            for key, val in node.items():
                # Skip if already handled as an alias
                if isinstance(val, str) and val.startswith(":="): continue
                
                if isinstance(val, dict) and len(val) > 0 and not any(k.startswith(('_', '$')) for k in val.keys()):
                    item[key] = self.execute(key, val, context=row_context)
                else:
                    item[key] = row[key]
            results.append(item)
        return results

if __name__ == "__main__":
    gql_query = """
    directors {
        id := dir_var,
        name,
        country : {"India", "USA"},
        movies {
            name,
            budget : [50, 100],
            director_id = dir_var
        }
    }
    """
    ast = ql_to_json(gql_query)
    root = list(ast.keys())[0]
    print(json.dumps(CSVAdapter().execute(root, ast[root]), indent=4))