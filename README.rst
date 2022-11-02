# databend-py

Databend CLoud Python Driver with native interface support


Features
========

- External data for query processing.

- TLS support.

- Query settings.

- Types support:

  * Float32/64
  * [U]Int8/16/32/64/128/256
  * Date/Date32/DateTime('timezone')/DateTime64('timezone')
  * String
  * Array(T)
  * Nullable(T)
  * Bool


Documentation
=============

// TODO


Usage
=====


Pure Client example:

    .. code-block:: python

        >>> from databend_driver import Client
        >>>
        >>> client = Client('http://localhost:8081')
        >>>
        >>> client.execute('SHOW TABLES')
        [('test',)]
        >>> client.execute('DROP TABLE IF EXISTS test')
        []
        >>> client.execute('CREATE TABLE test (x Int32)')
        []
        >>> client.execute(
        ...     'INSERT INTO test (x) VALUES', [(1,)]
        ... )
        1
        >>> client.execute('INSERT INTO test (x) VALUES', [(200,)])
        1
License
=======

ClickHouse Python Driver is distributed under the `Apache license`.
