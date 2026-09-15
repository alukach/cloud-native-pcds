`demo.sql` is the payoff: a handful of DuckDB queries that each touch a few MiB
of a multi-gigabyte dataset over HTTP range requests. Point `root` at your
published prefix and run `duckdb -init sql/demo.sql`.
