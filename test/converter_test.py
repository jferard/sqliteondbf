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
import sys
sys.path.insert(0, "..")
import sqliteondbf.converter as cv
import unittest
from unittest.mock import *
from itertools import zip_longest

class ConverterTest(unittest.TestCase):
    def setUp(self):
        self.__logger = Mock()
        self.__cursor = MagicMock()
        self.__dbf_table = MagicMock()

    def test_empty_empty(self):
        self.__dbf_table.name = "empty1"
        self.__dbf_table.fields = []
        self.__dbf_table.field_names = []
        self.__dbf_table.__iter__.return_value = []

        cv.SQLiteConverterWorker(self.__logger, self.__cursor, self.__dbf_table).import_dbf_file()

        # verify cursor
        expected = [
            call.execute('DROP TABLE IF EXISTS "empty1"'),
            call.execute('CREATE TABLE "empty1" ()'),
            call.executemany('INSERT INTO "empty1" VALUES ()', ANY),
            ANY
        ]
        for e, c in zip_longest(expected, self.__cursor.mock_calls):
            self.assertEquals(e, c)

        # verify logger
        expected = [
            call.debug('drop table SQL:\nDROP TABLE IF EXISTS "empty1"'),
            call.debug('create table SQL:\nCREATE TABLE "empty1" ()'),
            call.debug('populate table SQL:\nINSERT INTO "empty1" VALUES ()'),
            call.debug(ANY)]
        for e, c in zip_longest(expected, self.__logger.mock_calls):
            self.assertEquals(e, c)

    def test_one_value_table(self):
        f = Mock()
        f.type = "F"
        f.name = "f"

        self.__dbf_table.name = "one_value_table"
        self.__dbf_table.fields = [f]
        self.__dbf_table.field_names = ["f"]
        self.__dbf_table.__iter__.return_value = [[1]]

        # replay
        cv.SQLiteConverterWorker(self.__logger, self.__cursor, self.__dbf_table).import_dbf_file()

        # verify
        # verify cursor
        expected = [
            call.execute('DROP TABLE IF EXISTS "one_value_table"'),
            call.execute('CREATE TABLE "one_value_table" ("f" FLOAT)'),
            call.executemany('INSERT INTO "one_value_table" VALUES (?)', ANY),
            ANY
        ]
        for e, c in zip_longest(expected, self.__cursor.mock_calls):
            self.assertEquals(e, c)

        # verify logger
        expected = [
            call.debug('drop table SQL:\nDROP TABLE IF EXISTS "one_value_table"'),
            call.debug('create table SQL:\nCREATE TABLE "one_value_table" ("f" FLOAT)'),
            call.debug('populate table SQL:\nINSERT INTO "one_value_table" VALUES (?)'),
            call.debug(ANY)]
        for e, c in zip_longest(expected, self.__logger.mock_calls):
            self.assertEquals(e, c)
