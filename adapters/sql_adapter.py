from sqlalchemy import create_engine, MetaData, Table, select, and_, or_ # type: ignore
from sqlalchemy.orm import sessionmaker # type: ignore

class SQLAdapter:
    def __init__(self, connection_string):
        """
        Initializes the SQL engine and metadata.
        connection_string: e.g., 'sqlite:///database.db' or 'postgresql://user:pass@host/db'
        """
        self.engine = create_engine(connection_string)
        self.metadata = MetaData()
        self.Session = sessionmaker(bind=self.engine)

    def _get_operator_filters(self, column, logic):
        """Maps GQL operator keys to SQLAlchemy filter expressions."""
        filters = []
        if isinstance(logic, dict):
            for op, val in logic.items():
                if op == "__ge__": filters.append(column >= val)
                elif op == "__le__": filters.append(column <= val)
                elif op == "__gt__": filters.append(column > val)
                elif op == "__lt__": filters.append(column < val)
                elif op == "$or": filters.append(column.in_(val))
        return filters

    def execute(self, table_name, node, context=None):
        """
        Translates the AST node into a SQLAlchemy SELECT statement.
        Recursively handles nested joins based on variables in 'context'.
        """
        session = self.Session()
        # Reflect table structure from the database
        table = Table(table_name, self.metadata, autoload_with=self.engine)
        
        # 1. Prepare Projection (SELECT columns)
        stmt_cols = []
        local_aliases = {}
        for key, logic in node.items():
            if isinstance(logic, str) and logic.startswith(":="):
                # Handle Assignment/Alias: id := dir_var
                stmt_cols.append(table.c[key].label(key))
                local_aliases[logic[2:]] = key
            elif not isinstance(logic, dict) or any(k.startswith(('_', '$')) for k in logic.keys()):
                # Regular column or filtered column
                if key in table.c:
                    stmt_cols.append(table.c[key])
        
        stmt = select(*stmt_cols)

        # 2. Prepare Filters (WHERE clauses)
        where_clauses = []
        for key, logic in node.items():
            if key in table.c:
                # Handle Relational Match: director_id = dir_var
                if isinstance(logic, str) and logic.startswith("="):
                    var_name = logic[1:]
                    if context and var_name in context:
                        where_clauses.append(table.c[key] == context[var_name])
                
                # Handle Operator Logic: budget: {__ge__: 50}
                elif isinstance(logic, dict):
                    where_clauses.extend(self._get_operator_filters(table.c[key], logic))
        
        if where_clauses:
            stmt = stmt.where(and_(*where_clauses))

        # 3. Execute and handle Recursive Joins
        results = []
        db_results = session.execute(stmt).mappings().all()
        
        for row in db_results:
            item = dict(row)
            # Update context for child queries
            row_context = context.copy() if context else {}
            for var_name, col_name in local_aliases.items():
                row_context[var_name] = row[col_name]

            # Recurse for nested blocks (e.g., movies { ... })
            for key, logic in node.items():
                if isinstance(logic, dict) and not any(k.startswith(('_', '$')) for k in logic.keys()):
                    item[key] = self.execute(key, logic, context=row_context)
            
            results.append(item)
            
        session.close()
        return results