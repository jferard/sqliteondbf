# -*- coding: utf-8 -*-
"""sqliteondbf - SQLite on DBF
      Copyright (C) 2018 J. Férard <https://github.com/jferard>
   This file is part of sqliteondbf.
   sqliteondbf is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   sqliteondbf is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
   """

# Notes:
# * A part of this tool was inspired by https://github.com/olemb/dbfread/blob/master/examples/dbf2sqlite by Ole Martin Bjørndalen / UiT The Arctic University of Norway (under MIT licence)
# * The example files are adapted from https://www.census.gov/data/tables/2016/econ/stc/2016-annual.html (I didn't find a copyright, but this is fair use I believe)

import sqlite3
import logging
import os
import dbfread

class SQLiteConverter():
    """A converter from dbf to sqlite3"""
    def __init__(self, connection = sqlite3.connect(":memory:"), logger=logging.getLogger("sqliteondbf")):
        self.__connection = connection
        self.__logger = logger

    def import_dbf(self, dbf_path, lowernames=True, encoding="cp850", char_decode_errors="strict"):
        """Import a dbf database to the current sqlite connection"""
        cursor = self.__connection.cursor()

        for fpath in self.__dbf_files(dbf_path):
            self.__logger.info("import dbf file {}".format(fpath))
            dbf_table = dbfread.DBF(fpath, lowernames=lowernames, encoding=encoding, char_decode_errors=char_decode_errors)
            SQLiteConverterWorker(self.__logger, cursor, dbf_table).import_dbf_file()

        self.__connection.commit()

    def __dbf_files(self, dbf_path):
        for root, _, names in os.walk(dbf_path):
            for name in names:
                lext = os.path.splitext(name)[-1].lower()
                if lext == ".dbf":
                    yield os.path.join(root, name)

class SQLiteConverterWorker():
    """The worker: converts a dbf table and add the table to the current connection"""
    __TYPEMAP = {
        'F': 'FLOAT',
        'L': 'BOOLEAN',
        'I': 'INTEGER',
        'C': 'TEXT',
        'N': 'REAL',
        'M': 'TEXT',
        'D': 'DATE',
        'T': 'DATETIME',
        '0': 'INTEGER',
    }

    def __init__(self, logger, cursor, dbf_table):
        self.__logger = logger
        self.__cursor = cursor
        self.__dbf_table = dbf_table

    def import_dbf_file(self):
        """Import the file"""
        try:
            self.__add_sqlite_table()
        except UnicodeDecodeError as err:
            self.__logger.error("error {}".format(str(err)))

    def __add_sqlite_table(self):
        self.__drop_table()
        self.__create_table()
        self.__populate_table()

    def __drop_table(self):
        sql = 'DROP TABLE IF EXISTS "{}"'.format(self.__dbf_table.name)
        self.__logger.debug("drop table SQL:\n{}".format(sql))
        self.__cursor.execute(sql)

    def __create_table(self):
        fields = ['"{}" {}'.format(f.name, self.__field_type(f)) for f in self.__dbf_table.fields]
        sql = 'CREATE TABLE "{}" ({})'.format(self.__dbf_table.name, ', '.join(fields))
        self.__logger.debug("create table SQL:\n{}".format(sql))
        self.__cursor.execute(sql)

    def __field_type(self, f):
        return SQLiteConverterWorker.__TYPEMAP.get(f.type, 'TEXT')

    def __populate_table(self):
        placeholders = ", ".join(["?"]*len(self.__dbf_table.fields))
        sql = 'INSERT INTO "{}" VALUES ({})'.format(self.__dbf_table.name, placeholders)
        self.__logger.debug("populate table SQL:\n{}".format(sql))

        values = (list(rec.values()) for rec in self.__dbf_table)
        self.__cursor.executemany(sql, values)
        self.__logger.debug("rowcount: {}".format(self.__cursor.rowcount))
