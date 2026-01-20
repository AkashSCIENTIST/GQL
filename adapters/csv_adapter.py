import pandas as pd # type: ignore
import os

class CSVAdapter:
    def __init__(self, folder_path=".", verbose=True):
        self.folder_path = folder_path
        self.verbose = verbose

    def _log(self, msg):
        """Standardized logger for execution tracing."""
        if self.verbose: 
            print(f"[ADAPTER VERBOSE] {msg}")

    def _apply_filter(self, df, col, logic):
        """Applies numeric range logic without reindexing warnings."""
        if not isinstance(logic, dict) or df.empty: 
            return df
        
        ops = {"__ge__": ">=", "__gt__": ">", "__le__": "<=", "__lt__": "<"}
        
        for k, p_op in ops.items():
            if k in logic:
                lim = float(logic[k])
                # Recalculate col_num for the CURRENT state of df to keep indices aligned
                col_num = pd.to_numeric(df[col], errors='coerce')
                
                if p_op == ">=": df = df[col_num >= lim]
                elif p_op == ">": df = df[col_num > lim]
                elif p_op == "<=": df = df[col_num <= lim]
                elif p_op == "<": df = df[col_num < lim]
                
                self._log(f"      Filter {col} {p_op} {lim}: {len(df)} rows remain.")
        return df

    def execute(self, table_key, node, context=None):
        """
        Main execution engine. 
        - table_key: The key from the AST (could be the table name or an alias).
        - node: The AST segment for this table.
        - context: Variables passed down from parent rows (for joins).
        """
        meta = node.get("__meta__", {})
        # Use table_source to find the physical CSV, fallback to table_key
        table_source = meta.get("table_source", table_key)
        
        file_path = os.path.join(self.folder_path, f"{table_source}.csv")
        if not os.path.exists(file_path):
            self._log(f"File {file_path} missing.")
            return []
        
        # 1. Load data and sanitize (strip spaces, treat as strings)
        df = pd.read_csv(file_path, dtype=str)
        df.columns = df.columns.str.strip()
        # Normalize all cell values to stripped strings
        df = df.applymap(lambda x: str(x).strip() if pd.notnull(x) else "")
        
        context = context or {}

        # --- PHASE 1: FILTERING ---
        for k, l in node.items():
            if k == "__meta__" or k not in df.columns:
                continue
            
            # CASE 1: List Filtering (e.g., {"India", "USA"} -> ["India", "USA"])
            if isinstance(l, list):
                # strip surrounding quotes from list items for robust matching
                def _norm(x):
                    s = str(x)
                    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                        return s[1:-1]
                    return s
                nl = [_norm(x) for x in l]
                df = df[df[k].astype(str).isin([str(x) for x in nl])]
                self._log(f"      List Filter {k} IN {nl}: {len(df)} rows remain.")

            # CASE 2: Range Filtering (e.g., [50, 100])
            elif isinstance(l, dict):
                df = self._apply_filter(df, k, l)
                
            # CASE 3: Join Logic or Scalar Comparison
            elif isinstance(l, str):
                if l.startswith("="):
                    # Join: =dir_var
                    v_name = l[1:].split(":=")[0].strip()
                    val = str(context.get(v_name, "")).strip()
                    if val: 
                        df = df[df[k] == val]
                elif not l.startswith(":="):
                    # Scalar Equality - strip surrounding quotes if present
                    s = str(l)
                    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                        s = s[1:-1]
                    df = df[df[k].str.lower() == s.lower()]

        # --- PHASE 2: ITERATION & RECURSION ---
        results = []
        noise_keys = {"Using", "global", "variable"}
        valid_keys = [k for k in node.keys() if k not in noise_keys and (k in df.columns or (isinstance(node[k], dict) and "__meta__" in node[k]))]

        for _, row in df.iterrows():
            item, row_ctx, skip = {}, context.copy(), False
            
            # Populate context for children (important for joins)
            for k, l in node.items():
                if k in df.columns:
                    ctx_key = l[2:] if (isinstance(l, str) and l.startswith(":=")) else k
                    row_ctx[ctx_key] = row[k]

            # Build result fields
            for k in valid_keys:
                l = node[k]
                out_k = l.split(":=")[-1].strip() if (isinstance(l, str) and ":=" in l) else k
                
                if isinstance(l, dict) and "__meta__" in l:
                    # Recurse for sub-tables
                    sub_res = self.execute(k, l, context=row_ctx)
                    
                    # Handle STRICT (!) Logic
                    t_src = l['__meta__'].get('table_source', k)
                    if t_src in meta.get("strict_keys", []) and not sub_res:
                        skip = True; break
                    
                    alias = l["__meta__"].get("alias", k)
                    item[alias] = sub_res
                else:
                    item[out_k] = row[k]

            if not skip:
                # Cleanup INTERNAL (~) keys
                for int_k in meta.get("internal_keys", []):
                    pop_k = node[int_k].split(":=")[-1].strip() if ":=" in str(node[int_k]) else int_k
                    item.pop(pop_k, None)
                results.append(item)

        # --- PHASE 3: PLUCK (*) & DOUBLE PLUCK (**) ---
        pluck_level = meta.get("pluck", 0)

        if pluck_level > 0:
            # Build a 2D list where each inner list corresponds to the item's values
            final_plucked = []
            for item in results:
                row_values = []
                for val in item.values():
                    # DOUBLE PLUCK (**) will flatten nested lists into the inner row
                    if pluck_level == 2 and isinstance(val, list):
                        row_values.extend(val)
                    else:
                        row_values.append(val)
                # Always append as a list (preserve 2D shape for single-row results)
                final_plucked.append(row_values)

            # If double-pluck requested, flatten the 2D list into a single 1D list
            if pluck_level == 2:
                flat = []
                for rv in final_plucked:
                    if isinstance(rv, list):
                        flat.extend(rv)
                    else:
                        flat.append(rv)
                return flat

            return final_plucked

        return results