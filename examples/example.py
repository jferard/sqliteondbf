# coding: utf-8
""" sqliteondbf - SQLite on DBF
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

# Note: The example files are adapted from https://www.census.gov/data/tables/2016/econ/stc/2016-annual.html (I didn't find a copyright, but this is fair use I believe)

import logging
import sys

sys.path.insert(0, ".")
import sqliteondbf

logging.basicConfig(level=logging.INFO)
connection = sqliteondbf.connect(".")

cursor = connection.cursor()
cursor.execute("""SELECT
    SUM(amount)
FROM "2016-stc-detailed"
WHERE state_code = '0' AND item_code='T00' """)
amount = next(cursor)

print ("Total US: {}".format(amount[0]))

cursor.execute("""SELECT
    group_concat(item_name),
    SUM(amount)
FROM "2016-stc-detailed" d
LEFT JOIN item i
ON d.item_code = i.item_code
WHERE state_code = '0' AND d.item_code<>'T00' AND d.item_code NOT LIKE 'TA%' """)
amount = next(cursor)

print ("Total US ({}): {}".format(*amount))

cursor.execute("""SELECT
    group_concat(item_name),
    SUM(amount)
FROM "2016-stc-detailed" d
LEFT JOIN item i
ON d.item_code = i.item_code
WHERE state_code = '0' AND d.item_code IN ('T01', 'TA1', 'TA3', 'TA4', 'TA5') """)
amount = next(cursor)

print ("Total US ({}): {}".format(*amount))

cursor.execute("""SELECT
    group_concat(s.state_name),
    SUM(amount)
FROM "2016-stc-detailed" d
LEFT JOIN state s
ON d.state_code = s.state_code
WHERE d.state_code <> '0' AND item_code='T00' """)
amount = next(cursor)

print ("Total States ({}): {}".format(*amount))
