import re
import json

class GQLParser:
    def __init__(self, verbose=True):
        self.verbose = verbose
        self.token_specification = [
            ('STRICT',   r'!'), ('INTERNAL', r'~'), ('PLUCK',    r'\*'),
            ('ALIAS',    r':='), ('LBRACE',   r'\{'), ('RBRACE',   r'\}'),
            ('LBRACKET', r'\['), ('RBRACKET', r'\]'), ('COLON',    r':'), 
            ('EQ',       r'='), ('COMMA',    r','), 
            ('TABLE',    r'<[a-zA-Z_0-9]+>'), 
            ('ID',       r'\$?[a-zA-Z_][a-zA-Z0-9_\.]*'), 
            ('NUMBER',   r'-?\d+(\.\d+)?'), # Explicit number token
            ('SKIP',     r'[ \t\r]+'), ('NEWLINE',  r'\n'),
        ]
        self.re_tokens = "|".join(f"(?P<{name}>{pattern})" for name, pattern in self.token_specification)

    def _log(self, msg):
        if self.verbose: print(f"[MACRO VERBOSE] {msg}")

    def _smart_cast(self, val):
        v = str(val).strip().strip('"').strip("'")
        try:
            if '.' in v: return float(v)
            return int(v)
        except ValueError: return v

    def _format_range(self, val_str):
        val_str = str(val_str).strip()
        if (val_str.startswith('[') or val_str.startswith('(')) and ',' in val_str:
            op, cl = val_str[0], val_str[-1]
            parts = val_str[1:-1].split(',')
            return {
                ("__ge__" if op == '[' else "__gt__"): self._smart_cast(parts[0]), 
                ("__le__" if cl == ']' else "__lt__"): self._smart_cast(parts[1])
            }
        return self._smart_cast(val_str)

    def parse_variables_block(self):
        """Strictly captures values immediately following the colon."""
        d = {}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'RBRACE': 
                self.consume()
                break
            
            if k == 'ID':
                name = self.consume()[1]
                var_key = name if name.startswith('$') else f"${name}"
                
                if self.peek()[0] == 'COLON':
                    self.consume() # Consume COLON
                    # parse_expr starts exactly at the next token
                    val = self.parse_expr()
                    d[var_key] = val
                    self._log(f"Parsed Global: {var_key} raw value is '{val}'")
            
            elif k in ('COMMA', 'NEWLINE'):
                self.consume()
            else:
                self.pos += 1
        return d

    def parse_expr(self):
        """Collects tokens into a string until a boundary is hit."""
        res = []
        depth = 0
        while self.pos < len(self.tokens):
            k, v = self.peek()
            
            # Boundary check
            if depth == 0 and k in ('COMMA', 'NEWLINE', 'RBRACE'):
                break
            
            # Track nesting for ranges [,]
            if k in ('LBRACKET', 'LBRACE'): depth += 1
            if k in ('RBRACKET', 'RBRACE'): depth -= 1
            
            res.append(self.consume()[1])
        return "".join(res).strip()

    def resolve_macros(self, ast):
        if "__globals__" not in ast: return ast
        raw_macros = ast.pop("__globals__")
        resolved_strings = {}

        # 1. String-level resolution (replacing $vars inside other $vars)
        sorted_keys = sorted(raw_macros.keys(), key=len, reverse=True)
        
        def _get_deep_string(val):
            curr_val = str(val)
            for k in sorted_keys:
                if k in curr_val:
                    # Find what the variable points to
                    sub_content = raw_macros[k]
                    # If that content is also a variable, recurse
                    if isinstance(sub_content, str) and sub_content.startswith('$'):
                        sub_content = _get_deep_string(sub_content)
                    curr_val = curr_val.replace(k, str(sub_content))
            return curr_val

        self._log("Starting Global Resolution...")
        for k in sorted_keys:
            final_str = _get_deep_string(raw_macros[k])
            resolved_strings[k] = final_str
            self._log(f"  {k} -> {final_str}")

        # 2. Injection and Dictionary conversion
        def _inject(node):
            if isinstance(node, dict):
                return {k: _inject(v) for k, v in node.items()}
            if isinstance(node, str):
                # Is it a direct variable like "$movie_budget"?
                if node.startswith('$') and node in resolved_strings:
                    return self._format_range(resolved_strings[node])
                # Is it a string containing variables?
                if '$' in node:
                    for k, v in resolved_strings.items():
                        node = node.replace(k, str(v))
                    return self._format_range(node)
            return node

        return _inject(ast)

    # --- Basic Parser Logic ---
    def parse(self, code):
        self.tokens = list(self.tokenize(code))
        self.pos = 0
        raw_ast = {}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'ID' and v == "$global":
                self.consume(); self.consume() # $global {
                raw_ast["__globals__"] = self.parse_variables_block()
            elif k == 'TABLE':
                t_name = self.consume()[1][1:-1] # Get 'directors' from <directors>
                
                # --- NEW LOGIC TO CAPTURE TOP-LEVEL PLUCK ---
                is_pluck = False
                if self.peek()[0] == 'PLUCK':
                    self.consume() # Consume '*'
                    is_pluck = True
                
                if self.peek()[0] == 'LBRACE':
                    self.consume() # Consume '{'
                    node = self.parse_block()
                    node["__meta__"]["pluck"] = is_pluck # Set pluck correctly
                    raw_ast[t_name] = node
            else:
                self.pos += 1
        return self.resolve_macros(raw_ast)

    def parse_block(self):
        node = {"__meta__": {"strict_keys": [], "internal_keys": [], "pluck": False}}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'RBRACE': self.consume(); break
            s = i = p = False
            while self.peek()[0] in ('STRICT', 'INTERNAL'):
                m = self.consume()[0]
                if m == 'STRICT': s = True
                if m == 'INTERNAL': i = True
            k, v = self.peek()
            if k == 'TABLE':
                tn = self.consume()[1][1:-1]
                if s: node["__meta__"]["strict_keys"].append(tn)
                if self.peek()[0] == 'PLUCK': self.consume(); p = True
                self.consume(); sub = self.parse_block()
                sub["__meta__"]["pluck"] = p
                if self.peek()[0] == 'ALIAS': self.consume(); sub["__meta__"]["alias"] = self.consume()[1]
                node[tn] = sub
            elif k == 'ID':
                ident = self.consume()[1]
                if i: node["__meta__"]["internal_keys"].append(ident)
                nk, nv = self.peek()
                if nk == 'ALIAS': self.consume(); node[ident] = f":={self.consume()[1]}"
                elif nk == 'COLON': self.consume(); node[ident] = self.parse_expr()
                elif nk == 'EQ': self.consume(); node[ident] = f"={self.parse_expr()}"
                else: node[ident] = {}
            elif k in ('COMMA', 'NEWLINE'): self.consume()
            else: self.pos += 1
        return node

    def tokenize(self, code):
        for mo in re.finditer(self.re_tokens, code):
            if mo.lastgroup == 'SKIP': continue
            yield mo.lastgroup, mo.group()

    def peek(self, n=0): return self.tokens[self.pos+n] if self.pos+n < len(self.tokens) else (None, None)
    def consume(self): t=self.tokens[self.pos]; self.pos+=1; return t