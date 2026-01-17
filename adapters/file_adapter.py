import pandas as pd # type: ignore
from .base_adapter import BaseAdapter
import json

class FileAdapter(BaseAdapter):
    def __init__(self, source_path):
        self.source_path = source_path
        # Detect type based on extension
        if source_path.endswith('.csv'):
            self.df = pd.read_csv(source_path)
        elif source_path.endswith('.jsonl'):
            self.df = pd.read_json(source_path, lines=True)
        else:
            self.df = pd.read_json(source_path)

    def execute(self, query_ast):
        # 1. Identify table/root (e.g., 'movies')
        root_key = list(query_ast.keys())[0]
        node = query_ast[root_key]
        
        result_df = self.df.copy()

        # 2. Apply Filters (__args__)
        if "__args__" in node:
            for col, val in node["__args__"].items():
                if isinstance(val, str) and (val.startswith('[') or val.startswith('(')):
                    result_df = self._apply_interval(result_df, col, val)
                else:
                    result_df = result_df[result_df[col] == val]

        # 3. Apply Projection (Columns to keep)
        # Filters out internal keys like __args__ and __func__
        cols = [k for k in node.keys() if not k.startswith("__")]
        if cols:
            result_df = result_df[cols]

        return result_df.to_dict(orient='records')

    def _apply_interval(self, df, col, expr):
        opening, closing = expr[0], expr[-1]
        v1, v2 = [float(x.strip()) for x in expr[1:-1].split(',')]
        if opening == '[': df = df[df[col] >= v1]
        else: df = df[df[col] > v1]
        if closing == ']': df = df[df[col] <= v2]
        else: df = df[df[col] < v2]
        return df