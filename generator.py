import re
import json
import ast
import operator

class GQLParser:
    def __init__(self, verbose=True):
        self.verbose = verbose
        self._print_counter = 0
        self._pending_prints = []
        self.token_specification = [
            ('STRICT',   r'!'), 
            ('INTERNAL', r'~'), 
            ('PLUCK',    r'\*\*|\*'),       # Matches ** before *
            ('ALIAS',    r':='), 
            ('SHIFT',    r'<<|>>'),
            ('OP',       r'[\+\-\/\%\&\|\^~]+'),
            ('LPAREN',   r'\('),
            ('RPAREN',   r'\)'),
            ('STRING',   r"\"[^\"\\n]*\"|'[^'\\n]*'"),
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
            left_raw, right_raw = parts[0].strip(), parts[1].strip()
            # Try to evaluate arithmetic/bitwise expressions in range endpoints
            try:
                left_val = self._eval_expr(left_raw)
            except Exception:
                left_val = self._smart_cast(left_raw)
            try:
                right_val = self._eval_expr(right_raw)
            except Exception:
                right_val = self._smart_cast(right_raw)
            return {
                ("__ge__" if op == '[' else "__gt__"): left_val,
                ("__le__" if cl == ']' else "__lt__"): right_val
            }
        # Try to evaluate generic expressions (e.g. "$a * 2", "1 << 3")
        try:
            return self._eval_expr(val_str)
        except Exception:
            return self._smart_cast(val_str)

    def _eval_expr(self, expr_str):
        """Safely evaluate arithmetic and bitwise expressions.

        Supported operators: + - * / // % ** << >> & | ^ ~ and unary +/-. Uses Python AST for safety.
        """
        node = ast.parse(str(expr_str), mode='eval')

        def _eval(node):
            if isinstance(node, ast.Expression):
                return _eval(node.body)
            if isinstance(node, ast.Constant):
                return node.value
            if isinstance(node, ast.Num):
                return node.n
            if isinstance(node, ast.BinOp):
                left = _eval(node.left)
                right = _eval(node.right)
                op = node.op
                ops = {
                    ast.Add: operator.add,
                    ast.Sub: operator.sub,
                    ast.Mult: operator.mul,
                    ast.Div: operator.truediv,
                    ast.FloorDiv: operator.floordiv,
                    ast.Mod: operator.mod,
                    ast.Pow: operator.pow,
                    ast.LShift: operator.lshift,
                    ast.RShift: operator.rshift,
                    ast.BitAnd: operator.and_,
                    ast.BitOr: operator.or_,
                    ast.BitXor: operator.xor,
                }
                for t, fn in ops.items():
                    if isinstance(op, t):
                        return fn(left, right)
                raise ValueError(f"Unsupported operator: {ast.dump(op)}")
            if isinstance(node, ast.UnaryOp):
                operand = _eval(node.operand)
                if isinstance(node.op, ast.UAdd):
                    return +operand
                if isinstance(node.op, ast.USub):
                    return -operand
                if isinstance(node.op, ast.Invert):
                    return ~operand
                raise ValueError(f"Unsupported unary operator: {ast.dump(node.op)}")
            if isinstance(node, ast.Tuple):
                return tuple(_eval(e) for e in node.elts)
            # Disallow names, calls, attributes, subscripts, etc.
            raise ValueError(f"Unsupported expression node: {type(node).__name__}")

        return _eval(node)

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

        # Normalize braced string macros like {"India", "USA"} which
        # may have been tokenized imperfectly (missing leading/trailing quotes).
        def _normalize_braced_strings(s):
            try:
                t = str(s).strip()
                if not (t.startswith('{') and t.endswith('}')):
                    return s
                inner = t[1:-1]
                parts = []
                cur = []
                in_q = None
                for ch in inner:
                    if ch in ('"', "'"):
                        if in_q is None:
                            in_q = ch
                            cur.append(ch)
                            continue
                        elif in_q == ch:
                            cur.append(ch)
                            in_q = None
                            continue
                    if ch == ',' and in_q is None:
                        parts.append(''.join(cur).strip())
                        cur = []
                        continue
                    cur.append(ch)
                if cur:
                    parts.append(''.join(cur).strip())

                # Repair each part to ensure it is quoted
                fixed = []
                for p in parts:
                    if not p: continue
                    p = p.strip()
                    if (p.startswith('"') and p.endswith('"')) or (p.startswith("'") and p.endswith("'")):
                        fixed.append(p)
                        continue
                    # If endswith quote but missing start, add
                    if (p.endswith('"') or p.endswith("'")) and not (p.startswith('"') or p.startswith("'")):
                        fixed.append('"' + p[:-1] + '"')
                        continue
                    # If startswith quote but missing end, add
                    if (p.startswith('"') or p.startswith("'")) and not (p.endswith('"') or p.endswith("'")):
                        fixed.append(p + '"')
                        continue
                    # otherwise wrap in double quotes
                    fixed.append('"' + p.strip(' "\'') + '"')

                return '{' + ', '.join(fixed) + '}'
            except Exception:
                return s

        for k, v in list(resolved_strings.items()):
            if isinstance(v, str) and v.strip().startswith('{') and v.strip().endswith('}'):
                newv = _normalize_braced_strings(v)
                if newv != v:
                    resolved_strings[k] = newv
                    self._log(f"Normalized braced macro: {k} -> {resolved_strings[k]}")

        # Replace ':' with '=' only for global value macros (not for print(...) entries)
        for k, v in list(resolved_strings.items()):
            if isinstance(v, str) and not str(v).strip().lower().startswith('print'):
                resolved_strings[k] = v.replace(':', '=')

        # Execute any pending synthetic print entries created while parsing globals
        for pname in getattr(self, '_pending_prints', []):
            if pname not in raw_macros: continue
            try:
                s_raw = raw_macros[pname]
                # Use the same manual parsing logic as below but prefer raw macro values for $args
                pstart = s_raw.find('(')
                pend = s_raw.rfind(')')
                if pstart == -1 or pend == -1 or pend <= pstart:
                    continue
                inner = s_raw[pstart+1:pend]

                args_list = []
                cur = []
                depth = 0
                in_q = None
                for ch in inner:
                    if ch in ('"', "'"):
                        if in_q is None:
                            in_q = ch
                        elif in_q == ch:
                            in_q = None
                        cur.append(ch)
                        continue
                    if ch in '([{':
                        depth += 1
                    elif ch in ')]}':
                        depth -= 1
                    if ch == ',' and depth == 0 and in_q is None:
                        args_list.append(''.join(cur).strip())
                        cur = []
                    else:
                        cur.append(ch)
                if cur:
                    args_list.append(''.join(cur).strip())

                final_args = []
                final_kw = {}
                for part in args_list:
                    if not part:
                        continue
                    if '=' in part and part.split('=')[0].strip() in ('sep', 'end'):
                        k, v = part.split('=', 1)
                        v = v.strip()
                        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                            vval = v[1:-1]
                        else:
                            try:
                                vval = self._eval_expr(v)
                            except Exception:
                                vval = self._smart_cast(v)
                        final_kw[k.strip()] = vval
                        continue
                    # resolve macro variables: accept both $name and bare name
                    key = None
                    if part.startswith('$') and part in resolved_strings:
                        key = part
                    elif part.startswith('$') and part in raw_macros:
                        key = part
                    else:
                        candidate = f"${part}"
                        if candidate in resolved_strings or candidate in raw_macros:
                            key = candidate

                    if key is not None:
                        raw_val = resolved_strings.get(key, raw_macros.get(key))
                        # Try to evaluate ranges/lists or expressions
                        try:
                            if isinstance(raw_val, str) and (raw_val.startswith('[') or raw_val.startswith('(')):
                                final_args.append(self._format_range(raw_val) or self._smart_cast(raw_val))
                            else:
                                final_args.append(self._eval_expr(raw_val) if isinstance(raw_val, str) else raw_val)
                        except Exception:
                            final_args.append(self._smart_cast(raw_val))
                        continue
                        continue
                    if (part.startswith('"') and part.endswith('"')) or (part.startswith("'") and part.endswith("'")):
                        final_args.append(part[1:-1])
                        continue
                    try:
                        final_args.append(self._eval_expr(part))
                    except Exception:
                        final_args.append(self._smart_cast(part))

                print(*final_args, **{k: v for k, v in final_kw.items() if k in ('sep', 'end')})
                # Remove the synthetic print macro so it won't be executed again below
                raw_macros.pop(pname, None)
                resolved_strings.pop(pname, None)
            except Exception:
                continue

        # Execute any print(...) macros now (after macro resolution)
        for k, v in list(raw_macros.items()):
            if not isinstance(v, str):
                continue
            vs = resolved_strings.get(k, v)
            s = str(vs).strip()
            if not s.lower().startswith('print') or '(' not in s or not s.endswith(')'):
                continue

            for mk, mv in resolved_strings.items():
                s = s.replace(mk, str(mv))

            # Manual, safe parsing of print(...) so $macros are printed as their raw declaration
            try:
                pstart = s.find('(')
                pend = s.rfind(')')
                if pstart == -1 or pend == -1 or pend <= pstart:
                    continue
                inner = s[pstart+1:pend]

                # split args by commas respecting quotes and nested brackets
                args_list = []
                cur = []
                depth = 0
                in_q = None
                for ch in inner:
                    if ch in ('"', "'"):
                        if in_q is None:
                            in_q = ch
                        elif in_q == ch:
                            in_q = None
                        cur.append(ch)
                        continue
                    if ch in '([{':
                        depth += 1
                    elif ch in ')]}':
                        depth -= 1
                    if ch == ',' and depth == 0 and in_q is None:
                        args_list.append(''.join(cur).strip())
                        cur = []
                    else:
                        cur.append(ch)
                if cur:
                    args_list.append(''.join(cur).strip())

                final_args = []
                final_kw = {}
                for part in args_list:
                    if not part:
                        continue
                    # keyword arg
                    if '=' in part and part.split('=')[0].strip() in ('sep', 'end'):
                        k, v = part.split('=', 1)
                        v = v.strip()
                        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
                            vval = v[1:-1]
                        else:
                            try:
                                vval = self._eval_expr(v)
                            except Exception:
                                vval = self._smart_cast(v)
                        final_kw[k.strip()] = vval
                        continue

                    # macro variable -> print raw macro declaration (from raw_macros)
                    if part.startswith('$') and part in raw_macros:
                        raw = raw_macros[part]
                        final_args.append(str(raw))
                        continue

                    # quoted string
                    if (part.startswith('"') and part.endswith('"')) or (part.startswith("'") and part.endswith("'")):
                        final_args.append(part[1:-1])
                        continue

                    # fallback: try eval expr then smart cast
                    try:
                        final_args.append(self._eval_expr(part))
                    except Exception:
                        final_args.append(self._smart_cast(part))

                print(*final_args, **{k: v for k, v in final_kw.items() if k in ('sep', 'end')})
            except Exception:
                continue

        # Pass 2: Injection & Object Conversion (Ranges & Lists)
        def _inject(node):
            if isinstance(node, dict):
                return {k: _inject(v) for k, v in node.items()}
            if isinstance(node, str):
                s_orig = node
                s_strip = str(s_orig).strip()
                # Allow print(...) to be called anywhere in the AST; execute and remove
                if s_strip.lower().startswith('print') and '(' in s_strip and s_strip.endswith(')'):
                    # substitute resolved globals into the print string
                    s = s_strip
                    for mk, mv in resolved_strings.items():
                        s = s.replace(mk, str(mv))
                    try:
                        expr = ast.parse(s, mode='eval')
                        call = expr.body
                        if isinstance(call, ast.Call) and isinstance(call.func, ast.Name) and call.func.id == 'print':
                            def _eval_print_node(node):
                                if isinstance(node, ast.Constant):
                                    return node.value
                                if isinstance(node, ast.Name):
                                    # Map bare names to resolved globals if available
                                    name = node.id
                                    key = f"${name}"
                                    if key in resolved_strings:
                                        raw_val = resolved_strings[key]
                                        try:
                                            return self._eval_expr(raw_val) if isinstance(raw_val, str) else raw_val
                                        except Exception:
                                            return self._smart_cast(raw_val)
                                    return name
                                if isinstance(node, ast.Num):
                                    return node.n
                                if isinstance(node, ast.Str):
                                    return node.s
                                if isinstance(node, ast.Tuple):
                                    return tuple(_eval_print_node(e) for e in node.elts)
                                if isinstance(node, ast.List):
                                    return [_eval_print_node(e) for e in node.elts]
                                if isinstance(node, ast.BinOp):
                                    left = _eval_print_node(node.left)
                                    right = _eval_print_node(node.right)
                                    op = node.op
                                    ops = {
                                        ast.Add: operator.add,
                                        ast.Sub: operator.sub,
                                        ast.Mult: operator.mul,
                                        ast.Div: operator.truediv,
                                        ast.FloorDiv: operator.floordiv,
                                        ast.Mod: operator.mod,
                                        ast.Pow: operator.pow,
                                        ast.LShift: operator.lshift,
                                        ast.RShift: operator.rshift,
                                        ast.BitAnd: operator.and_,
                                        ast.BitOr: operator.or_,
                                        ast.BitXor: operator.xor,
                                    }
                                    for t, fn in ops.items():
                                        if isinstance(op, t):
                                            return fn(left, right)
                                    raise ValueError(f"Unsupported operator in print arg: {ast.dump(op)}")
                                if isinstance(node, ast.UnaryOp):
                                    val = _eval_print_node(node.operand)
                                    if isinstance(node.op, ast.UAdd):
                                        return +val
                                    if isinstance(node.op, ast.USub):
                                        return -val
                                    if isinstance(node.op, ast.Invert):
                                        return ~val
                                    raise ValueError(f"Unsupported unary op in print arg: {ast.dump(node.op)}")
                                raise ValueError(f"Unsupported node in print arg: {type(node).__name__}")

                            args = [_eval_print_node(a) for a in call.args]
                            kwds = {kw.arg: _eval_print_node(kw.value) for kw in call.keywords}
                            print(*args, **{k: v for k, v in kwds.items() if k in ("sep", "end")})
                            return None
                    except Exception:
                        # non-fatal; ignore print errors and fall through
                        pass
                # 1. Check for explicit list syntax {A, B}
                lst = self._format_list(node)
                if lst: return lst

                # 2. Check for macro-based values
                if node.startswith('$') and node in resolved_strings:
                    raw_val = resolved_strings[node]
                    # Check if macro content is a list
                    lst = self._format_list(raw_val)
                    if lst: return lst
                    # Try to evaluate arithmetic/bitwise expressions first
                    try:
                        return self._eval_expr(raw_val)
                    except Exception:
                        return self._format_range(raw_val)
                
                # 3. Check for inline variables in strings
                if '$' in node:
                    for k, v in resolved_strings.items():
                        node = node.replace(k, str(v))
                    # Re-check for list after replacement
                    lst = self._format_list(node)
                    if lst: return lst
                    try:
                        return self._eval_expr(node)
                    except Exception:
                        return self._format_range(node)
            return node

        return _inject(ast)

    def parse_variables_block(self):
        d = {}
        while self.pos < len(self.tokens):
            k, v = self.peek()
            if k == 'RBRACE': self.consume(); break
            if k == 'ID':
                ident = v
                var_key = v if v.startswith('$') else f"${v}"
                self.consume()
                # If this is a bare print(...) call inside $global, capture it as a synthetic macro
                if ident == 'print' and self.peek()[0] == 'LPAREN':
                    # parse the parenthesized expression (parse_expr will consume parens and contents)
                    expr_tail = self.parse_expr()
                    # reconstruct the full print call
                    full = f"{ident}{expr_tail}"
                    pname = f"$__print_{self._print_counter}"
                    self._print_counter += 1
                    d[pname] = full
                    self._pending_prints.append(pname)
                elif self.peek()[0] == 'COLON':
                    self.consume(); d[var_key] = self.parse_expr(stop_on_id_colon=True)
            elif k in ('COMMA', 'NEWLINE'): self.consume()
            else: self.pos += 1
        return d

    def parse_expr(self, stop_on_id_colon=False):
        res, depth = [], 0
        while self.pos < len(self.tokens):
            k, v = self.peek()
            # allow stopping when next token is an ID followed by COLON (for globals)
            if stop_on_id_colon and depth == 0 and k == 'ID' and self.peek(1)[0] == 'COLON':
                break
            if depth == 0 and k in ('COMMA', 'NEWLINE', 'RBRACE'): break
            if k in ('LBRACKET', 'LBRACE', 'LPAREN'): depth += 1
            if k in ('RBRACKET', 'RBRACE', 'RPAREN'): depth -= 1
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
        table_counts = {}
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
                has_alias = False
                if self.peek()[0] == 'ALIAS':
                    self.consume(); ast_key = self.consume()[1]
                    node["__meta__"]["alias"] = ast_key
                    has_alias = True

                if not has_alias:
                    cnt = table_counts.get(t_source, 0)
                    if cnt == 1:
                        existing = raw_ast.get(t_source)
                        if existing and not existing.get("__meta__", {}).get("alias"):
                            raw_ast[f"{t_source}_0"] = existing
                            del raw_ast[t_source]
                    if cnt > 0:
                        ast_key = f"{t_source}_{cnt}"

                table_counts[t_source] = table_counts.get(t_source, 0) + 1

                raw_ast[ast_key] = node
            else: self.pos += 1
        return self.resolve_macros(raw_ast)

    def tokenize(self, code):
        for mo in re.finditer(self.re_tokens, code):
            if mo.lastgroup == 'SKIP': continue
            yield mo.lastgroup, mo.group()

    def peek(self, n=0): return self.tokens[self.pos+n] if self.pos+n < len(self.tokens) else (None, None)
    def consume(self): t=self.tokens[self.pos]; self.pos+=1; return t