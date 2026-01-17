import pandas as pd # type: ignore
import os

class CSVAdapter:
    def __init__(self, folder_path="."):
        self.folder_path = folder_path

    def _apply_filter(self, df, col, logic):
        """Applies filters while keeping the index synchronized."""
        if not isinstance(logic, dict): 
            return df
            
        try:
            # 1. Set Filtering
            if "$or" in logic:
                allowed = [str(i) for i in logic["$or"]]
                df = df[(df[col].astype(str).isin(allowed)) | 
                        (pd.to_numeric(df[col], errors='coerce').isin(logic["$or"]))]
            
            # 2. Range Filtering 
            # We call pd.to_numeric(df[col]) inside each filter to ensure 
            # the index matches the current (potentially already filtered) df.
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
        if not os.path.exists(file_path): return []
        
        df = pd.read_csv(file_path)
        meta = node.get("__meta__", {"strict_keys": [], "internal_keys": []})

        # 1. Prune rows based on filters
        for key, logic in node.items():
            if key == "__meta__": continue
            if key in df.columns:
                if isinstance(logic, str) and logic.startswith("="):
                    var_name = logic[1:].split(" ")[0]
                    val = context.get(var_name) if context else None
                    if val is not None: df = df[df[key].astype(str) == str(val)]
                elif isinstance(logic, dict) and any(k.startswith(('_', '$')) for k in logic.keys() if k != "__meta__"):
                    df = self._apply_filter(df, key, logic)

        results = []
        for _, row in df.iterrows():
            item = {}
            row_context = context.copy() if context else {}
            skip_row = False
            
            # 2. Build result item
            for key, logic in node.items():
                if key == "__meta__": continue
                
                out_key = key
                if isinstance(logic, str) and ":=" in logic:
                    out_key = logic.split(":=")[-1].strip()

                # REFINED LOGIC: 
                # A subtable is a dict that contains keys OTHER THAN __meta__ 
                # AND does not contain operator keys like __ge__ or $or.
                is_subtable = (isinstance(logic, dict) and 
                               any(k != "__meta__" for k in logic.keys()) and 
                               not any(k.startswith(('_', '$')) for k in logic.keys() if k != "__meta__"))

                if is_subtable:
                    sub_data = self.execute(key, logic, context=row_context)
                    # ! STRICT CHECK
                    if key in meta["strict_keys"] and not sub_data:
                        skip_row = True
                        break
                    item[out_key] = sub_data
                else:
                    # It's a regular column or a simple attribute
                    if key in df.columns:
                        val = row[key]
                        item[out_key] = val
                        # Ensure assignments (:=) are saved for context
                        if isinstance(logic, str) and ":=" in logic:
                            row_context[out_key] = val

            if skip_row: continue

            # 3. ~ INTERNAL CHECK: Strip hidden fields
            for int_key in meta["internal_keys"]:
                actual_out_key = int_key
                l_val = node.get(int_key)
                if isinstance(l_val, str) and ":=" in l_val:
                    actual_out_key = l_val.split(":=")[-1].strip()
                item.pop(actual_out_key, None)

            results.append(item)
            
        return results