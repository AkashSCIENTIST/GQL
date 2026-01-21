import polars as pl # type: ignore
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
        if not isinstance(logic, dict) or (hasattr(df, 'is_empty') and df.is_empty()):
            return df

        ops = {"__ge__": ">=", "__gt__": ">", "__le__": "<=", "__lt__": "<"}

        for k, p_op in ops.items():
            if k in logic:
                lim = float(logic[k])
                # Use Polars vectorized comparisons after casting to Float64
                if p_op == ">=":
                    df = df.filter(pl.col(col).cast(pl.Float64) >= lim)
                elif p_op == ">":
                    df = df.filter(pl.col(col).cast(pl.Float64) > lim)
                elif p_op == "<=":
                    df = df.filter(pl.col(col).cast(pl.Float64) <= lim)
                elif p_op == "<":
                    df = df.filter(pl.col(col).cast(pl.Float64) < lim)

                # polars DataFrame length
                try:
                    remaining = df.height
                except Exception:
                    remaining = len(df)
                self._log(f"      Filter {col} {p_op} {lim}: {remaining} rows remain.")
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
        
        # 1. Load data and sanitize (strip spaces, treat as strings) using polars
        df = pl.read_csv(file_path)
        # Normalize column names (strip surrounding spaces)
        new_cols = [c.strip() for c in df.columns]
        if new_cols != list(df.columns):
            df = df.rename({old: new for old, new in zip(list(df.columns), new_cols)})
        # Cast all columns to Utf8 and strip string values
        # Use .apply with return_dtype for compatibility across polars versions
        df = df.with_columns([
                        pl.col(c)
                            .cast(pl.Utf8)
                            .map_elements(lambda s: s.strip() if s is not None else "", return_dtype=pl.Utf8)
                            .alias(c)
            for c in df.columns
        ])
        
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
                df = df.filter(pl.col(k).is_in([str(x) for x in nl]))
                try:
                    remaining = df.height
                except Exception:
                    remaining = len(df)
                self._log(f"      List Filter {k} IN {nl}: {remaining} rows remain.")

            # CASE 2: Range Filtering (e.g., [50, 100])
            elif isinstance(l, dict):
                df = self._apply_filter(df, k, l)
                
            # CASE 3: Join Logic or Scalar Comparison
            elif isinstance(l, str):
                if l.startswith("="):
                    # Join: =dir_var
                    v_name = l[1:].split(":=")[0].strip()
                    val = str(context.get(v_name, "")).strip()
                    # Fallback: if the named context variable isn't present, try common parent id
                    if not val:
                        val = str(context.get('id', '')).strip()
                    if val: 
                        df = df.filter(pl.col(k) == val)
                elif not l.startswith(":="):
                    # Scalar Equality - strip surrounding quotes if present
                    s = str(l)
                    if (s.startswith('"') and s.endswith('"')) or (s.startswith("'") and s.endswith("'")):
                        s = s[1:-1]
                    # perform case-insensitive match via map_elements to normalize
                    df = df.filter(pl.col(k).map_elements(lambda x: x.lower() if x is not None else "", return_dtype=pl.Utf8) == s.lower())

        # --- PHASE 2: ITERATION & RECURSION ---
        results = []
        noise_keys = {"Using", "global", "variable"}
        # valid keys include columns, nested table nodes (with __meta__), and function nodes (with __func__)
        valid_keys = [
            k for k in node.keys()
            if k not in noise_keys and (
                k in df.columns or
                (isinstance(node[k], dict) and ("__meta__" in node[k] or "__func__" in node[k]))
            )
        ]

        # Convert to list of dicts for row-wise processing (keeps behavior consistent)
        rows = df.to_dicts()
        for row in rows:
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
                elif isinstance(l, dict) and "__func__" in l:
                    # Function handling (e.g., count)
                    func_name = l.get("__func__")
                    arg = l.get("__arg__")
                    if isinstance(arg, dict):
                        # determine subtable key to call execute: prefer alias then table_source
                        sub_key = arg.get("__meta__", {}).get("alias") or arg.get("__meta__", {}).get("table_source")
                        sub_res = self.execute(sub_key, arg, context=row_ctx)
                    else:
                        # if arg is expression string, evaluate or skip
                        sub_res = []

                    if func_name == 'count':
                        if isinstance(sub_res, list):
                            item[out_k] = len(sub_res)
                        else:
                            item[out_k] = 1 if sub_res else 0
                    elif func_name == 'sum':
                        # Sum a numeric column from the sub-res rows.
                        # Determine target field from the function arg's requested keys (ignore __meta__ and internal keys)
                        total = 0.0
                        if isinstance(arg, dict) and isinstance(sub_res, list):
                            sub_meta = arg.get('__meta__', {})
                            internal = set(sub_meta.get('internal_keys', []))
                            candidate_fields = [k for k in arg.keys() if k != '__meta__' and k not in internal]
                            if candidate_fields:
                                field = candidate_fields[0]
                                for r in sub_res:
                                    try:
                                        v = r.get(field, 0)
                                        # accept numbers in strings and remove commas
                                        if isinstance(v, str):
                                            v = v.replace(',', '')
                                        total += float(v)
                                    except Exception:
                                        # ignore non-numeric or missing values
                                        continue
                                # prefer int when possible
                                if total.is_integer():
                                    item[out_k] = int(total)
                                else:
                                    item[out_k] = total
                            else:
                                item[out_k] = 0
                        else:
                            item[out_k] = 0
                    else:
                        # unknown function: return raw result
                        item[out_k] = sub_res
                else:
                    item[out_k] = row.get(k)

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