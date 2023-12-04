# databend-py

Databend Cloud Python Driver with native http interface support

[![image](https://img.shields.io/pypi/v/databend-py.svg)](https://pypi.org/project/databend-py)

[![image](https://coveralls.io/repos/github/databendcloud/databend-py/badge.svg?branch=master)](https://coveralls.io/github/databendcloud/databend-py?branch=master)

[![image](https://img.shields.io/pypi/l/databend-py.svg)](https://pypi.org/project/databend-py)

[![image](https://img.shields.io/pypi/pyversions/databend-py.svg)](https://pypi.org/project/databend-py)

# Installation

pip install databend-py

# Usage

Use the next code to check connection:

> ``` python
> >>> from databend_py import Client
> >>> client = Client(
>     host='tenant--warehouse.ch.datafusecloud.com',
>     database="default",
>     user="user",
>     port="443",
>     password="password",settings={"copy_purge":True,"force":True})
> >>> print(client.execute("SELECT 1"))
> ```

The [host]{.title-ref}, [user]{.title-ref}, [password]{.title-ref} info
will be found in databend cloud warehouse connect page as flows:

Pure Client example:

> ``` python
> >>> from databend_py import Client
> >>>
> >>> client = Client.from_url('http://root@localhost:8000/db?secure=False&copy_purge=True')
> >>>
> >>> client.execute('SHOW TABLES')
> [[], [('test',)]]
> >>> client.execute("show tables",with_column_types=True)
> [[('Tables_in_default', 'String')], [('test',)]] # [[(column_name, column_type)], [(data,)]]
> >>> client.execute('DROP TABLE IF EXISTS test')
> []
> >>> client.execute('CREATE TABLE test (x Int32)')
> []
> >>> client.execute(
> ...     'INSERT INTO test (x) VALUES', [(1,)]
> ... )
> 1
> >>> client.execute('INSERT INTO test (x) VALUES', [(200,)])
> 1
> ```

More usages examples find [here](./examples).

# Features

-   Basic SQL.
-   TLS support.
-   Query settings.
-   Types support:
    -   Float32/64
    -   \[U\]Int8/16/32/64/128/256
    -   Date/Date32/DateTime(\'timezone\')/DateTime64(\'timezone\')
    -   String
    -   Array(T)
    -   Nullable(T)
    -   Bool

# Compatibility

-   If databend version \>= v0.9.0 or later, you need to use databend-py
    version \>= v0.3.0.

# License

Databend Python Driver is distributed under the [Apache
license]{.title-ref}.
