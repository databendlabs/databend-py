databend-py
===========

Databend Cloud Python Driver with native http interface support

.. image:: https://img.shields.io/pypi/v/databend-py.svg
    :target: https://pypi.org/project/databend-driver

.. image:: https://coveralls.io/repos/github/databendcloud/databend-py/badge.svg?branch=master
    :target: https://coveralls.io/github/databendcloud/databend-py?branch=master

.. image:: https://img.shields.io/pypi/l/databend-driver.svg
    :target: https://pypi.org/project/databend-py
    

.. image:: https://img.shields.io/pypi/pyversions/databend-py.svg
    :target: https://pypi.org/project/databend-py

Installation
============

pip install databend-py

Usage
=====

Use the next code to check connection:

  .. code-block:: python

    >>> from databend_py import Client
    >>> client = Client(
        host='tenant--warehouse.ch.datafusecloud.com',
        database="default",
        user="user",
        password="password")
    >>> print(client.execute("SELECT 1"))

The `host`, `user`, `password` info will be found in databend cloud warehouse connect page as flows:


Pure Client example:

    .. code-block:: python

        >>> from databend_pyimport Client
        >>>
        >>> client = Client('http://root:rootlocalhost:8081/db')
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

Features
========

- Basic SQL.

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

License
=======

Databend Python Driver is distributed under the `Apache license`.
