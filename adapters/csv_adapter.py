import pandas as pd # type: ignore
import os

class CSVAdapter:
    def __init__(self, folder_path="."):
        self.folder_path = folder_path

    def _apply_filter(self, df, col, logic):
        """Applies mathematical range and set filters with dynamic index alignment."""
        if not isinstance(logic, dict): 
            return df
        try:
            # 1. Set/Equality Matching
            if "$or" in logic:
                allowed = [str(i) for i in logic["$or"]]
                # Dual-type check: Match against string or numeric values in CSV
                df = df[
                    (df[col].astype(str).isin(allowed)) | 
                    (pd.to_numeric(df[col], errors='coerce').isin(logic["$or"]))
                ]
            
            # 2. Mathematical Range Matching (Fixed to prevent UserWarnings)
            if "__ge__" in logic: 
                df = df[pd.to_numeric(df[col], errors='coerce') >= float(logic["__ge__"])]
            if "__gt__" in logic: 
                df = df[pd.to_numeric(df[col], errors='coerce') > float(logic["__gt__"])]
            if "__le__" in logic: 
                df = df[pd.to_numeric(df[col], errors='coerce') <= float(logic["__le__"])]
            if "__lt__" in logic: 
                df = df[pd.to_numeric(df[col], errors='coerce') < float(logic["__lt__"])]
        except Exception as e:
            print(f"Filter error on column {col}: {e}")
        return df

    def execute(self, table_name, node, context=None):
        file_path = os.path.join(self.folder_path, f"{table_name}.csv")
        if not os.path.exists(file_path): 
            return []
        
        df = pd.read_csv(file_path)
        meta = node.get("__meta__", {"strict_keys": [], "internal_keys": [], "pluck": False})

        # --- PHASE 1: FILTERING ---
        for key, logic in node.items():
            if key == "__meta__": continue
            if key in df.columns:
                if isinstance(logic, str) and logic.startswith("="):
                    var_name = logic[1:].split(" ")[0]
                    val = context.get(var_name) if context else None
                    if val is not None:
                        df = df[df[key].astype(str) == str(val)]
                elif isinstance(logic, dict) and any(k.startswith(('_', '$')) for k in logic.keys() if k != "__meta__"):
                    df = self._apply_filter(df, key, logic)

        # --- PHASE 2: PROCESSING ROWS ---
        results = []
        for _, row in df.iterrows():
            item = {}
            row_context = context.copy() if context else {}
            skip_row = False
            
            for key, logic in node.items():
                if key == "__meta__": continue
                
                out_key = key
                if isinstance(logic, str) and ":=" in logic:
                    out_key = logic.split(":=")[-1].strip()

                is_subtable = (isinstance(logic, dict) and 
                               any(k != "__meta__" for k in logic.keys()) and 
                               not any(k.startswith(('_', '$')) for k in logic.keys() if k != "__meta__"))

                if is_subtable:
                    block_meta = logic.get("__meta__", {})
                    out_key = block_meta.get("alias", key)
                    
                    sub_data = self.execute(key, logic, context=row_context)
                    if key in meta.get("strict_keys", []) and not sub_data:
                        skip_row = True
                        break
                    item[out_key] = sub_data
                else:
                    if key in df.columns:
                        val = row[key]
                        item[out_key] = val
                        if isinstance(logic, str) and ":=" in logic:
                            row_context[out_key] = val

            if skip_row: continue

            # --- PHASE 3: INTERNAL KEY STRIPPING ---
            for int_key in meta.get("internal_keys", []):
                actual_out_key = int_key
                l_val = node.get(int_key)
                if isinstance(l_val, str) and ":=" in l_val:
                    actual_out_key = l_val.split(":=")[-1].strip()
                item.pop(actual_out_key, None)

            results.append(item)

        # --- PHASE 4: PLUCK (FLATTENING) ---
        # If pluck is true and exactly one field remains, convert [{key: val}] to [val]
        if meta.get("pluck", False) and results:
            if all(len(obj) == 1 for obj in results):
                return [list(obj.values())[0] for obj in results]
            
        return results