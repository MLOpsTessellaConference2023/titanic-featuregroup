# titanic-featuregroup





## Further work

The following are things to consider when creating ML workflows that we didn't in this project to be even to
simplify it:

- All key or string values must be externalized into constant variables to avoid problems caused by misspelling
- CSV files are not the most efficient storage format, use parquet or h5 files instead
- For bigger datasets use other technologies like Apache Arrow (PyArrow), Apache Spark (PySpark), Dask ...
- Include linting and testing in the CI/CD workflows
