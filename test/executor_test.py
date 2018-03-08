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
import sqliteondbf.executor as ex
import unittest

class ExecutorTest(unittest.TestCase):
    def testEmpty(self):
        executor = ex.SQLiteExecutor("/* first script */")
        executor.execute()

    def testPrint(self):
        executor = ex.SQLiteExecutor("$print ok")
        executor.execute()

    def testDump(self):
        executor = ex.SQLiteExecutor("$dump a")
        self.assertRaises(Exception, executor.execute)

    def testView(self):
        executor = ex.SQLiteExecutor("$view -1")
        self.assertRaises(Exception, executor.execute)

    def testExport(self):
        executor = ex.SQLiteExecutor("$export a")
        self.assertRaises(Exception, executor.execute)

    def testDef(self):
        executor = ex.SQLiteExecutor("$def f():return 1")
        self.assertRaises(Exception, executor.execute)

    def testAggregate(self):
        executor = ex.SQLiteExecutor("$aggregate f():\n\tdef step(self):pass")
        self.assertRaises(Exception, executor.execute)

    def testSQL(self):
        executor = ex.SQLiteExecutor("SQL")
        self.assertRaises(Exception, executor.execute)

if __name__ == '__main__':
    unittest.main()
