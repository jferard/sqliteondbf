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
import sqliteondbf.splitter as sp
import unittest

class SplitterTest(unittest.TestCase):
    def test(self):
        text="""
        a semicolon "in a double quoted (\\") string;" is not a separation
        this is one;
        a semicolon 'in a single quoted (\\') string;' is not a separation
        this is one;
        -- a semicolon in a line comment is not a separation;
        this is one;
        /* a semicolon in a block comment
        is not a separation; */
        this is one;
        """

        l = list(sp.Splitter().split(text))
        self.assertEquals(
            ['a semicolon "in a double quoted (\\") string;" is not a separation\n        this is one',
            "a semicolon 'in a single quoted (\\') string;' is not a separation\n        this is one",
            'this is one',
            'this is one'], l)

if __name__ == '__main__':
    unittest.main()
