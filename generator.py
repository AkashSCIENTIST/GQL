import re
import json

class GQLParser:
    def __init__(self, verbose=True):
        self.verbose = verbose
        self.token_specification = [
            ('STRICT',   r'!'), 
            ('INTERNAL', r'~'), 
            ('PLUCK',    r'\*\*|\*'),       # Matches ** before *
            ('ALIAS',    r':='), 
            ('LBRACE',   r'\{'), 
            ('RBRACE',   r'\}'),
            ('LBRACKET', r'\['), 
            ('RBRACKET', r'\]'), 
            ('COLON',    r':'), 
            ('EQ',       r'='), 
            ('COMMA',    r','), 
            ('TABLE',    r'<[a-zA-Z_0-9]+>'), 
            ('ID',       r'\$?[a-zA-Z_][a-zA-Z0-9_\.]*'), 
            ('NUMBER',   r'-?\d+(\.\d+)?'), 
            ('SKIP',     r'[ \t\r]+'), 
            ('NEWLINE',  r'\n'),
        ]
        self.re_tokens = "|".join(f"(?P<{name}>{pattern})" for name, pattern in self.token_specification)

    def _log(self, msg):
        if self.verbose: print(f"[PARSER VERBOSE] {msg}")

    def _smart_cast(self, val):
        v = str(val).strip().strip('"').strip("'")
        try:
            return float(v) if '.' in v else int(v)
        except ValueError: return v

    def _format_range(self, val_str):
        """Converts '[min, max]' into a range dict."""
        val_str = str(val_str).strip()
        if (val_str.startswith('[') or val_str.startswith('(')) and ',' in val_str:
            op, cl = val_str[0], val_str[-1]
            parts = val_str[1:-1].split(',')
            return {
                ("__ge__" if op == '[' else "__gt__"): self._smart_cast(parts[0]), 
                ("__le__" if cl == ']' else "__lt__"): self._smart_cast(parts[1])
            }
        return self._smart_cast(val_str)

    def _format_list(self, val_str):
        """Converts '{val1, val2}' into a Python list."""
        val_str = str(val_str).strip()
        if val_str.startswith('{') and val_str.endswith('}'):
            parts = val_str[1:-1].split(',')
            return [self._smart_cast(p.strip()) for p in parts if p.strip()]
        return None

    def resolve_macros(self, ast):
        if "__globals__" not in ast: return ast
        raw_macros = ast.pop("__globals__")
        resolved_strings = {}

        # Pass 1: Recursive String Resolution
        def _get_deep_string(val):
            curr_val = str(val)
            for k in sorted(raw_macros.keys(), key=len, reverse=True):
                if k in curr_val:
                    sub_content = raw_macros[k]
                    if isinstance(sub_content, str) and sub_content.startswith('$'):
                        sub_content = _get_deep_string(sub_content)
                    curr_val = curr_val.replace(k, str(sub_content))
            return curr_val

        for k in raw_macros:
            resolved_strings[k] = _get_deep_string(raw_macros[k])
            self._log(f"Macro Resolved: {k} -> {resolved_strings[k]}")

        # Pass 2: Injection & Object Conversion (Ranges & Lists)
        def _inject(node):
            if isinstance(node, dict):
                return {k: _inject(v) for k, v in node.items()}
            if isinstance(node, str):
                # 1. Check for explicit list syntax {A, B}
                lst = self._format_list(node)
                if lst: return lst

                # 2. Check for macro-based values
                if node.startswith('$') and node in resolved_strings:
                    raw_val = resolved_strings[node]
                    # Check if macro content is a list
                    lst = self._format_list(raw_val)
                    if lst: return lst
                    # Otherwise treat as range or scalar
                    return self._format_range(raw_val)
                
                # 3. Check for inline variables in strings
                if '$' in node:
                    for k, v in resolved_strings.items():
                        node = node.replace(k, str(v))
                    # Re-check for list after replacement
                    lst = self._format_list(node)
                    if lst: return lst
                    return self._format_range(node)
            return node

        return _inject(ast)

    def parse_variables_block(self):
        d = {}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'RBRACE': self.consume(); break
            if k == 'ID':
                var_key = v if v.startswith('$') else f"${v}"
                self.consume()
                if self.peek()[0] == 'COLON':
                    self.consume(); d[var_key] = self.parse_expr()
            elif k in ('COMMA', 'NEWLINE'): self.consume()
            else: self.pos += 1
        return d

    def parse_expr(self):
        res, depth = [], 0
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if depth == 0 and k in ('COMMA', 'NEWLINE', 'RBRACE'): break
            if k in ('LBRACKET', 'LBRACE'): depth += 1
            if k in ('RBRACKET', 'RBRACE'): depth -= 1
            res.append(self.consume()[1])
        return "".join(res).strip()

    def parse_block(self):
        node = {"__meta__": {"strict_keys": [], "internal_keys": [], "pluck": 0}}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'RBRACE': self.consume(); break
            
            strict = internal = False
            while self.peek()[0] in ('STRICT', 'INTERNAL'):
                m = self.consume()[0]
                if m == 'STRICT': strict = True
                if m == 'INTERNAL': internal = True

            k, v = self.peek()
            if k == 'TABLE':
                t_source = self.consume()[1][1:-1]
                if strict: node["__meta__"]["strict_keys"].append(t_source)
                
                sub_pluck = 0
                if self.peek()[0] == 'PLUCK':
                    sub_pluck = len(self.consume()[1])
                
                self.consume() # {
                sub = self.parse_block()
                sub["__meta__"]["pluck"] = sub_pluck
                sub["__meta__"]["table_source"] = t_source
                
                ast_key = t_source
                if self.peek()[0] == 'ALIAS':
                    self.consume(); ast_key = self.consume()[1]
                    sub["__meta__"]["alias"] = ast_key
                
                node[ast_key] = sub
            
            elif k == 'ID':
                ident = self.consume()[1]
                if internal: node["__meta__"]["internal_keys"].append(ident)
                nk, nv = self.peek()
                if nk == 'ALIAS':
                    self.consume(); node[ident] = f":={self.consume()[1]}"
                elif nk == 'COLON':
                    self.consume(); node[ident] = self.parse_expr()
                elif nk == 'EQ':
                    self.consume(); node[ident] = f"={self.parse_expr()}"
                else: node[ident] = {}
            
            elif k in ('COMMA', 'NEWLINE'): self.consume()
            else: self.pos += 1
        return node

    def parse(self, code):
        self.tokens = list(self.tokenize(code))
        self.pos = 0
        raw_ast = {}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'ID' and v == "$global":
                self.consume(); self.consume()
                raw_ast["__globals__"] = self.parse_variables_block()
            elif k == 'TABLE':
                t_source = self.consume()[1][1:-1]
                top_pluck = 0
                if self.peek()[0] == 'PLUCK':
                    top_pluck = len(self.consume()[1])
                
                self.consume() # {
                node = self.parse_block()
                node["__meta__"]["pluck"] = top_pluck
                node["__meta__"]["table_source"] = t_source
                
                ast_key = t_source
                if self.peek()[0] == 'ALIAS':
                    self.consume(); ast_key = self.consume()[1]
                    node["__meta__"]["alias"] = ast_key
                
                raw_ast[ast_key] = node
            else: self.pos += 1
        return self.resolve_macros(raw_ast)

    def tokenize(self, code):
        for mo in re.finditer(self.re_tokens, code):
            if mo.lastgroup == 'SKIP': continue
            yield mo.lastgroup, mo.group()

    def peek(self, n=0): return self.tokens[self.pos+n] if self.pos+n < len(self.tokens) else (None, None)
    def consume(self): t=self.tokens[self.pos]; self.pos+=1; return t