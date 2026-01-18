from generator import GQLParser

def ql_to_json(gql): 
    return GQLParser().parse(gql)

def resolve_macros(ast):
    """
    Static Resolution Pass: Acts like a C Preprocessor.
    Replaces all variable references with their values and removes __globals__.
    """
    if "__globals__" not in ast:
        return ast
    
    macros = ast.pop("__globals__") # Remove block so Adapter doesn't look for CSV

    def substitute(node):
        if isinstance(node, dict):
            # Recurse through dictionary keys and values
            return {k: substitute(v) for k, v in node.items()}
        elif isinstance(node, list):
            # Recurse through lists
            return [substitute(x) for x in node]
        elif isinstance(node, str):
            # If the string matches a macro name exactly, replace it
            if node in macros:
                return macros[node]
            # Handle cases where logic is combined like '=target_country'
            # (Though usually variables are standalone in your AST)
        return node

    # Apply substitution to the remaining data blocks
    return substitute(ast)