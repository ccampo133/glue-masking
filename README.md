# glue-masking

Example AWS Glue job that masks sensitive data in a PostgreSQL database.

## Overview

TODO.

## Miscellaneous

The current architecture executes one ETL job per table. Alternatively, you can
use a thread pool to execute multiple ETLs concurrently within a single job, as
shown below:

```python
# ...job boilerplate...

def etl(table):
    # ETL code here
    return f'{table} ETL done!'


# These will probably be parameters.
threads = 4
tables = ['table1', 'table2', 'table3', 'table4']

from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=threads) as executor:
    # ETL one table per thread.
    for result in executor.map(etl, tables):
        if result:
            print(result)

job.commit()
```

In preliminary testing, it is a bit slower than having concurrent jobs running,
but it is also simpler to manage, and may be more cost-effective. The optimal
approach will depend on the specific use case.
