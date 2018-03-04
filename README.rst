===========================
sqliteondbf - SQLite on DBF
===========================

Copyright (C) J. Férard 2018

SQLite on DBF is a very simple tool that allows to get a sqlite3 connection or to process a SQL script (sqlite flavour) on a set of dBase tables (dbf files).
Under GPL v.3

Notes:

* A part of this tool was inspired by https://github.com/olemb/dbfread/blob/master/examples/dbf2sqlite by Ole Martin Bjørndalen / UiT The Arctic University of Norway (under MIT licence)
* The example files are adapted from https://www.census.gov/data/tables/2016/econ/stc/2016-annual.html (I didn't find a copyright, but this is fair use I believe)

------------
Installation
------------
Windows/Linux:

.. code:: bash

    [sudo] python setup.py install

-------
Example
-------
As a script processor
=====================
In the ``sqliteondbf`` directory:

.. code:: bash

    python sqliteondbf.py -v examples/example.sql

It's also possible to execute an inline command (see below for the ``$convert`` command):

.. code:: bash

    python sqliteondbf.py -e "$convert 'examples' example.db 'utf-8'"

On Linux, please escape the ``$`` sign:

.. code:: bash

    python sqliteondbf.py -e "\$convert 'examples' example.db 'utf-8'"

This will convert all the dbf files in the ``examples`` directory and subdirectories into a sqlite3 databas names ``example.db``.

As a module
===========

In a python script (see examples/examples.py):

.. code:: python

    import sqliteondbf

    logging.basicConfig(level=logging.INFO)
    connection = sqliteondbf.connect("path/to/dbf/dir")

    # now use the sqlite3 connection as usual

----------
The script
----------
There is a mandatory blank line between instructions, but no semi colon is needed.

SQL instructions
================
Usual SQL (sqlite flavour) instructions are simply executed on the current connection.

Special instructions
====================
There are four special instructions that begins with a ``$`` sign: ``connect``, ``convert``, ``export``, ``def``.

``connect``
-----------
To use a set of dbf files, type:

.. code:: sql

    $connect dbf path/to/files/ [encoding]

The current connection is set to an in-memory database which contains all dbf tables.

To use an existing sqlite database a source, type:

.. code:: sql

    $connect sqlite path/to/sqlite.db

The current connection is set to a slite database. This is equivalent to ``sqlite3.connect("path/to/sqlite.db")`` in a python script.

``convert``
-----------
Similar to connect, but for saving the sqlite database

.. code:: sql

    $convert path/to/files/ path/to/sqlite.db [encoding]

The current connection to the database is set to the new sqlite database.

``export``
----------
Save the result of the last select to a csv file:

.. code:: sql

    $export file.csv

If the result was already fetched, the query is rerun.

``def``
-------
To use a custom python function in the script:

.. code:: sql

    $def func(args):
        ...
        return ret

``aggregate``
-------------
To use a custom python aggregate function in the script:

.. code:: sql

    $aggregate Class():
        def __init__(self):
            ...
        def step(self, v):
            ...
        def finalize(self):
            return ret

Beware: do not put any blank line between methods.

``dump``
--------
Make a dump of the base:

.. code:: sql

    $dump fname.sql

``view``
--------
Print the result of the last select on the terminal:

.. code:: sql

    $view [limit]

An optional argument ``limit`` sets the maximum number of rows to display. If ``limit`` is omitted, the its value is ``100``. If ``limit == -1``, then no limit is set.

If the result was already fetched, the query is rerun.

``print``
---------
Print a string or a list of string on the terminal

.. code:: sql

    $print something
