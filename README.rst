databend-py
===========

Databend Cloud Python Driver with native http interface support

.. image:: https://img.shields.io/pypi/v/databend-driver.svg
    :target: https://pypi.org/project/databend-driver

.. image:: https://coveralls.io/repos/github/databendcloud/databend-py/badge.svg?branch=master
    :target: https://coveralls.io/github/databendcloud/databend-py?branch=master

.. image:: https://img.shields.io/pypi/l/databend-driver.svg
    :target: https://pypi.org/project/databend-driver

.. image:: https://img.shields.io/pypi/pyversions/databend-driver.svg
    :target: https://pypi.org/project/databend-driver

Installation
============

pip install databend-py

Usage
=====

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
