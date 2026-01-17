import re
import json

class GQLParser:
    def __init__(self):
        self.token_specification = [
            ('STRICT',   r'!'),                     
            ('INTERNAL', r'~'),                     
            ('ALIAS',    r':='),                    
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
        # Handle Ranges
        if (val.startswith('[') or val.startswith('(')) and ',' in val:
            op, cl = val[0], val[-1]
            p = val[1:-1].split(',')
            try:
                v1, v2 = float(p[0].strip()), float(p[1].strip())
                if v1 == v2: return {"$or": [v1]}
                return {("__ge__" if op == '[' else "__gt__"): v1, ("__le__" if cl == ']' else "__lt__"): v2}
            except: return val
        # Handle Sets
        if val.startswith('{') and val.endswith('}'):
            items = val[1:-1].split(',')
            # Normalize types: Numbers to float, strings stripped of quotes
            cleaned = []
            for i in items:
                i = i.strip()
                if not i: continue
                try: cleaned.append(float(i))
                except: cleaned.append(i.strip('"\''))
            return {"$or": cleaned}
        return val

    def parse(self, code):
        tokens = list(self.tokenize(code))
        self.pos = 0

        def peek(n=0): return tokens[self.pos + n] if self.pos + n < len(tokens) else (None, None)
        def consume(): 
            val = tokens[self.pos]; self.pos += 1
            return val

        def parse_expr():
            res = []; depth = 0
            while self.pos < len(tokens):
                k, v = peek()
                if k in ('LBRACE', 'LPAREN', 'LBRACKET'): depth += 1
                if k in ('RBRACE', 'RPAREN', 'RBRACKET'):
                    if depth == 0: break
                    depth -= 1
                if k == 'COMMA' and depth == 0: break
                res.append(consume()[1])
            return "".join(res)

        def is_table_block():
            """Look ahead to distinguish between {Table} and {Set}."""
            # If next is an ID followed by { : = or , it's a table
            k1, v1 = peek(0) # The token after '{'
            k2, v2 = peek(1)
            if k1 == 'ID' and k2 in ('COLON', 'ALIAS', 'EQ', 'LBRACE', 'COMMA', 'RBRACE'):
                return True
            if k1 in ('STRICT', 'INTERNAL'):
                return True
            return False

        def parse_block():
            node = {"__meta__": {"strict_keys": [], "internal_keys": []}}
            while self.pos < len(tokens):
                k, v = peek()
                if k == 'RBRACE': consume(); break
                
                is_strict = is_internal = False
                if k == 'STRICT': consume(); is_strict = True; k, v = peek()
                if k == 'INTERNAL': consume(); is_internal = True; k, v = peek()
                
                if k == 'ID':
                    ident = consume()[1]
                    if is_strict: node["__meta__"]["strict_keys"].append(ident)
                    if is_internal: node["__meta__"]["internal_keys"].append(ident)
                    
                    nk, nv = peek()
                    if nk == 'COLON':
                        consume()
                        # CRITICAL FIX: Distinguish between block and set
                        if peek()[0] == 'LBRACE' and is_table_block():
                            consume() # eat {
                            node[ident] = parse_block()
                        else:
                            node[ident] = self._format_val(parse_expr())
                    elif nk == 'EQ':
                        consume(); m_var = consume()[1]; node[ident] = f"={m_var}"
                        if peek()[0] == 'ALIAS': consume(); node[ident] += f" :={consume()[1]}"
                    elif nk == 'ALIAS':
                        consume(); node[ident] = f":={consume()[1]}"
                    elif nk == 'LBRACE':
                        consume(); node[ident] = parse_block()
                    else:
                        node[ident] = {}
                elif k == 'COMMA': consume()
                else: self.pos += 1
            return node

        # Start parsing from the root
        while self.pos < len(tokens):
            k, v = peek()
            if k == 'ID':
                root = consume()[1]
                if peek()[0] == 'LBRACE':
                    consume()
                    return {root: parse_block()}
            consume()
        return {}

def ql_to_json(gql_string):
    return GQLParser().parse(gql_string)