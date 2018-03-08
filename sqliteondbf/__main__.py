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

import logging
import argparse

from executor import SQLiteExecutor as _SQLiteExecutor, connect, convert, export, view, dump

def execute(script, logger=logging.getLogger("sqliteondbf")):
    _SQLiteExecutor(script, logger).execute()

def _get_args():
    parser = argparse.ArgumentParser(
        description='Execute a sqlite3 script on a DBF base')

    parser.add_argument("script", nargs='?', help='a sqlite3 script')
    parser.add_argument("-v", "--verbose", action="store_true", help='enable verbose mode')
    parser.add_argument("-q", "--quiet", action="store_true", help='enable quiet mode')
    parser.add_argument("-e", action="store", metavar='program', help='execute program')

    return parser.parse_args()

def main():
    args = _get_args()
    if args.quiet:
        logging.basicConfig(level=logging.ERROR)
    elif args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger("sqliteondbf")

    if args.e and args.script:
        print ("Choose between -e and script")

    if args.e:
        _SQLiteExecutor(args.e, logger).execute()
    else:
        with open(args.script, 'r', encoding='utf-8') as source:
            _SQLiteExecutor(source, logger).execute()

if __name__ == '__main__':
    main()
