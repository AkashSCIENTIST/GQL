import re
import json

class GQLParser:
    def __init__(self):
        self.token_specification = [
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
            ('STRING',   r'"[^"]*"|\'[^\']*\''),    
            ('NUMBER',   r'-?\d+(\.\d+)?'),         
            ('SKIP',     r'[ \t\n\r]+'),            
        ]
        self.re_tokens = "|".join(f"(?P<{name}>{pattern})" for name, pattern in self.token_specification)

    def tokenize(self, code):
        for mo in re.finditer(self.re_tokens, code):
            if mo.lastgroup == 'SKIP': continue
            yield mo.lastgroup, mo.group()

    def _format_arg_value(self, val):
        val = val.strip()
        # Range logic: [50, 100]
        if val.startswith(('[', '(')) and ',' in val:
            op, cl = val[0], val[-1]
            p = val[1:-1].split(',')
            v1, v2 = float(p[0].strip().strip('"\'')), float(p[1].strip().strip('"\''))
            return { ("__ge__" if op == '[' else "__gt__"): v1, ("__le__" if cl == ']' else "__lt__"): v2 }
        # Set logic: {"A", "B"}
        if val.startswith('{') and val.endswith('}'):
            items = val[1:-1].split(',')
            return {"$or": [i.strip().strip('"').strip("'") for i in items if i.strip()]}
        return val

    def parse(self, code):
        tokens = list(self.tokenize(code))
        self.pos = 0

        def peek(n=0): return tokens[self.pos + n] if self.pos + n < len(tokens) else (None, None)
        def consume(): 
            val = tokens[self.pos]; self.pos += 1
            return val

        def parse_collection(end_type):
            res = []
            while self.pos < len(tokens):
                k, v = peek()
                if k == end_type: res.append(consume()[1]); break
                res.append(consume()[1])
            return "".join(res)

        def parse_block():
            node = {}
            while self.pos < len(tokens):
                k, v = peek()
                if k == 'RBRACE': consume(); break
                if k == 'ID':
                    ident = consume()[1]
                    nk, _ = peek()
                    
                    # id := dir_var
                    if nk == 'ALIAS':
                        consume(); node[ident] = f":={consume()[1]}"
                    
                    # country : {"India", "USA"} or budget : [50, 100]
                    elif nk == 'COLON':
                        consume()
                        vk, _ = peek()
                        if vk == 'LBRACE':
                            # Lookahead to see if it's a nested table or a filter set
                            if peek(1)[0] in ('COLON', 'ALIAS', 'EQ', 'LBRACE', 'ID'):
                                consume(); node[ident] = parse_block()
                            else:
                                raw = consume()[1] + parse_collection('RBRACE')
                                node[ident] = self._format_arg_value(raw)
                        elif vk in ('LPAREN', 'LBRACKET'):
                            s = consume()[1]
                            raw = s + parse_collection('RBRACKET' if s == '[' else 'RPAREN')
                            node[ident] = self._format_arg_value(raw)
                        else:
                            node[ident] = consume()[1].strip('"\'')
                    
                    # director_id = dir_var
                    elif nk == 'EQ':
                        consume()
                        node[ident] = f"={consume()[1]}"
                    
                    # movies { ... }
                    elif nk == 'LBRACE':
                        consume(); node[ident] = parse_block()
                    
                    # name (bare attribute)
                    else:
                        node[ident] = {}
                else: self.pos += 1
            return node

        while self.pos < len(tokens):
            k, v = peek()
            if k == 'ID':
                root = consume()[1]
                if peek()[0] == 'LBRACE': consume(); return {root: parse_block()}
            else: consume()
        return {}

def ql_to_json(gql_string):
    return GQLParser().parse(gql_string)

# Example Execution
if __name__ == "__main__":
    q = """
    directors {
        id := dir_var,
        name,
        country : {"India", "USA"},
        movies {
            title := name,
            budget : [50, 100],
            director_id = dir_var
        }
    }
    """
    print(json.dumps(ql_to_json(q), indent=4))



# {
#     "directors": {
#         "id": ":=dir_var",
#         "name": {},
#         "country": {},
#         "movies": {
#             "title": ":=name",
#             "budget": {},
#             "director_id": {},
#             "budget": {
#                 "__ge__": 50.0,
#                 "__le__": 100.0
#             },
#             "director_id": "=dir_var"  
#         },
#         "country": {
#             "$or": [
#                 "India",
#                 "USA"
#             ]
#         }
#     }
# }