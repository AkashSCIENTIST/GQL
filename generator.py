import re
import json

class GQLParser:
    def __init__(self):
        self.token_specification = [
            ('STRICT',   r'!'),                     # !Table
            ('INTERNAL', r'~'),                     # ~Field
            ('PLUCK',    r'\*'),                    # *{
            ('ALIAS',    r':='),                    # :=
            ('LBRACE',   r'\{'),                    
            ('RBRACE',   r'\}'),                    
            ('LPAREN',   r'\('),                    
            ('RPAREN',   r'\)'),                    
            ('LBRACKET', r'\['),                    
            ('RBRACKET', r'\]'),                    
            ('COLON',    r':'),                     
            ('EQ',       r'='),                     
            ('COMMA',    r','),                     
            ('ID',       r'[a-zA-Z_][a-zA-Z0-9_\.]*'), 
            ('NUMBER',   r'-?\d+(\.\d+)?'),         
            ('STRING',   r'"[^"]*"|\'[^\']*\''),    
            ('SKIP',     r'[ \t\n\r]+'),            
        ]
        self.re_tokens = "|".join(f"(?P<{name}>{pattern})" for name, pattern in self.token_specification)

    def tokenize(self, code):
        for mo in re.finditer(self.re_tokens, code):
            if mo.lastgroup == 'SKIP': continue
            yield mo.lastgroup, mo.group()

    def _format_val(self, val):
        val = val.strip()
        if not val: return {}
        # Range logic
        if (val.startswith('[') or val.startswith('(')) and ',' in val:
            op, cl = val[0], val[-1]
            parts = val[1:-1].split(',')
            try:
                v1, v2 = float(parts[0].strip()), float(parts[1].strip())
                if v1 == v2: return {"$or": [v1]}
                return {("__ge__" if op == '[' else "__gt__"): v1, ("__le__" if cl == ']' else "__lt__"): v2}
            except: return val
        # Set logic
        if val.startswith('{') and val.endswith('}'):
            items = val[1:-1].split(',')
            cleaned = []
            for i in items:
                item = i.strip().strip('"').strip("'")
                if not item: continue
                try: cleaned.append(float(item) if '.' in item else int(item))
                except: cleaned.append(item)
            return {"$or": cleaned}
        return val

    def parse(self, code):
        self.tokens = list(self.tokenize(code))
        self.pos = 0
        if not self.tokens: return {}
        k, v = self.peek()
        if k == 'ID':
            root_name = self.consume()[1]
            if self.peek()[0] == 'LBRACE':
                self.consume()
                return {root_name: self.parse_block()}
        return {}

    def peek(self, n=0):
        return self.tokens[self.pos + n] if self.pos + n < len(self.tokens) else (None, None)

    def consume(self):
        val = self.tokens[self.pos]
        self.pos += 1
        return val

    def is_table_block(self):
        """Checks if the current '{' belongs to a nested table or a data set."""
        k1, v1 = self.peek(0) 
        k2, v2 = self.peek(1)
        if k1 == 'ID' and k2 in ('COLON', 'ALIAS', 'EQ', 'LBRACE', 'COMMA', 'RBRACE', 'PLUCK'):
            return True
        if k1 in ('STRICT', 'INTERNAL', 'PLUCK'):
            return True
        return False

    def parse_expr(self):
        """Depth-aware expression parsing to capture [min, max] ranges fully."""
        res, depth = [], 0
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k in ('LBRACE', 'LPAREN', 'LBRACKET'): depth += 1
            elif k in ('RBRACE', 'RPAREN', 'RBRACKET'):
                if depth == 0: break
                depth -= 1
            if k == 'COMMA' and depth == 0: break
            res.append(self.consume()[1])
        return "".join(res)

    def parse_block(self):
        """Parses a table block including markers like !, ~, and *."""
        node = {"__meta__": {"strict_keys": [], "internal_keys": [], "pluck": False}}
        
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'RBRACE':
                self.consume()
                break
            
            is_strict = is_internal = is_pluck = False
            if k == 'STRICT': self.consume(); is_strict = True; k, v = self.peek()
            if k == 'INTERNAL': self.consume(); is_internal = True; k, v = self.peek()
            
            if k == 'ID':
                ident = self.consume()[1]
                if is_strict: node["__meta__"]["strict_keys"].append(ident)
                if is_internal: node["__meta__"]["internal_keys"].append(ident)
                
                nk, nv = self.peek()
                
                # Check for Pluck marker right after ID: movies *{
                if nk == 'PLUCK':
                    self.consume() 
                    is_pluck = True
                    nk, nv = self.peek()

                if nk == 'COLON':
                    self.consume()
                    if self.peek()[0] == 'LBRACE' and self.is_table_block():
                        self.consume() 
                        node[ident] = self.parse_block()
                        if is_pluck: node[ident]["__meta__"]["pluck"] = True
                        if self.peek()[0] == 'ALIAS':
                            self.consume(); node[ident]["__meta__"]["alias"] = self.consume()[1]
                    else:
                        node[ident] = self._format_val(self.parse_expr())
                
                elif nk == 'EQ':
                    self.consume()
                    m_var = self.consume()[1]
                    node[ident] = f"={m_var}"
                    if self.peek()[0] == 'ALIAS':
                        self.consume(); node[ident] += f" :={self.consume()[1]}"
                
                elif nk == 'ALIAS':
                    self.consume(); node[ident] = f":={self.consume()[1]}"
                
                elif nk == 'LBRACE':
                    self.consume()
                    node[ident] = self.parse_block()
                    if is_pluck: node[ident]["__meta__"]["pluck"] = True
                    if self.peek()[0] == 'ALIAS':
                        self.consume(); node[ident]["__meta__"]["alias"] = self.consume()[1]
                else:
                    node[ident] = {}
                    
            elif k == 'COMMA': self.consume()
            else: self.pos += 1
                
        return node

def ql_to_json(gql_string):
    return GQLParser().parse(gql_string)