import pandas as pd # type: ignore
import os

class CSVAdapter:
    def __init__(self, folder_path=".", verbose=True):
        self.folder_path = folder_path
        self.verbose = verbose

    def _log(self, msg):
        """Internal logger for execution tracing."""
        if self.verbose: 
            print(f"[ADAPTER VERBOSE] {msg}")

    def _apply_filter(self, df, col, logic):
        """Applies numeric range logic (>=, <=, >, <) to a DataFrame column."""
        if not isinstance(logic, dict): 
            return df
        
        # Convert column to numeric for mathematical comparison
        col_num = pd.to_numeric(df[col], errors='coerce')
        ops = {"__ge__": ">=", "__gt__": ">", "__le__": "<=", "__lt__": "<"}
        
        for k, p_op in ops.items():
            if k in logic:
                lim = float(logic[k])
                if p_op == ">=": df = df[col_num >= lim]
                elif p_op == ">": df = df[col_num > lim]
                elif p_op == "<=": df = df[col_num <= lim]
                elif p_op == "<": df = df[col_num < lim]
                self._log(f"      Filter {col} {p_op} {lim}: {len(df)} rows remain.")
        return df

    def execute(self, table_name, node, context=None):
        """
        Main execution engine. 
        Handles CSV loading, Filtering, Joins, and Recursive sub-table calls.
        """
        file_path = os.path.join(self.folder_path, f"{table_name}.csv")
        if not os.path.exists(file_path): 
            self._log(f"File {file_path} missing.")
            return []
        
        # Load data as strings and strip whitespace
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.strip()
        df = df.map(lambda x: str(x).strip() if pd.notnull(x) else "")
        
        meta = node.get("__meta__", {})
        context = context or {}

        # --- PHASE 1: FILTERING PASS ---
        for k, l in node.items():
            # Skip metadata and keys that don't exist in CSV (like noise keys or sub-tables)
            if k == "__meta__" or k not in df.columns:
                continue
            
            # Skip pure assignments (id := dir_var) from filtering
            if isinstance(l, str) and l.startswith(":="):
                continue
            
            # Case 1: Range logic (Dictionaries)
            if isinstance(l, dict):
                df = self._apply_filter(df, k, l)
            
            # Case 2: Join Logic (=dir_var)
            elif isinstance(l, str) and l.startswith("="):
                # Extract variable name (supports =var:=alias format)
                v_name = l[1:].split(":=")[0].strip()
                val = str(context.get(v_name, "")).strip()
                if val:
                    df = df[df[k] == val]
                    self._log(f"      Join {k} == {val}: {len(df)} rows remain.")
            
            # Case 3: Scalar Match (India, 25, etc.)
            elif l != {}:
                df = df[df[k].str.lower() == str(l).lower()]

        # --- PHASE 2: ITERATION & RECURSION ---
        results = []
        for _, row in df.iterrows():
            item, row_ctx, skip = {}, context.copy(), False
            
            # A. PRE-MAP: Populate row_ctx with all available columns for children
            for k, l in node.items():
                if k in df.columns:
                    # If aliased (id := dir_var), store value as 'dir_var'
                    ctx_key = l[2:] if (isinstance(l, str) and l.startswith(":=")) else k
                    row_ctx[ctx_key] = row[k]

            # B. EXECUTE: Build final fields and sub-tables
            for k, l in node.items():
                if k == "__meta__": continue
                
                # Determine output key (handle ALIAS :=)
                out_k = l.split(":=")[-1].strip() if (isinstance(l, str) and ":=" in l) else k
                
                # If the value is a nested dictionary with metadata, it's a sub-table
                if isinstance(l, dict) and "__meta__" in l:
                    sub_res = self.execute(k, l, context=row_ctx)
                    
                    # STRICT (!) handling: drop parent if child is empty
                    if k in meta.get("strict_keys", []) and not sub_res:
                        skip = True; break
                    
                    sub_alias = l["__meta__"].get("alias", k)
                    item[sub_alias] = sub_res
                
                # Otherwise, it's a field projection
                elif k in df.columns:
                    item[out_k] = row[k]

            # C. CLEANUP: Filter out skipped rows and internal (~) keys
            if not skip:
                for int_k in meta.get("internal_keys", []):
                    # Remove the field by its final output name
                    pop_k = node[int_k].split(":=")[-1].strip() if ":=" in str(node[int_k]) else int_k
                    item.pop(pop_k, None)
                results.append(item)

        # --- PHASE 3: PLUCK (*) ---
        # Converts list of dicts to list of values
        if meta.get("pluck") and results:
            plucked_results = []
            for item in results:
                vals = list(item.values())
                # If a row has only 1 field, take the scalar value
                # If a row has multiple fields, take the list of values
                if len(vals) == 1:
                    plucked_results.append(vals[0])
                else:
                    plucked_results.append(vals)
            
            # SPECIAL CASE: If there is only one row and it has multiple values,
            # flatten it to a single list: ["Singam", "Action"]
            if len(plucked_results) == 1 and isinstance(plucked_results[0], list):
                return plucked_results[0]
                
            return plucked_results
            
        return results