import polars as pl
print([m for m in dir(pl.col('x').str) if not m.startswith('__')])
