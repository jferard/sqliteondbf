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
import sqlite3
import csv

from splitter import SemicolonSplitter as _SemicolonSplitter
from converter import SQLiteConverter as _SQLiteConverter

class _SQLiteExecutor():
    def __init__(self, script, logger):
        if type(script) == str:
            self.__script = script
        else:
            self.__script = script.read()
        self.__logger = logger
        self.__ex = {
            "connect":self.__connect,
            "convert":self.__convert,
            "export":self.__export,
            "def":self.__def,
            "print":self.__print,
            "view":self.__view,
            "aggregate":self.__aggregate,
            "dump":self.__dump,
        }

    def execute(self):
        for e in _SemicolonSplitter().split(self.__script):
            e = e.strip()
            if not e:
                continue

            if e.startswith("$"):
                args = self.__get_args(e[1:]) # get arg
                self.__ex[args[0]](e[1:], *(args[1:]))
            else:
                try:
                    self.__cursor
                except:
                    if e.startswith("/*") or e.startswith("--"):
                        self.__logger.debug("ignore:\n{}".format(e))
                    else:
                        msg = "open a data source before executing instructions!! {} ignored".format(e)
                        self.__logger.error(msg)
                        raise Exception(msg)
                else:
                    if e.startswith("/*") or e.startswith("--"):
                        self.__logger.debug("ignore:\n{}".format(e))
                    else:
                        self.__last_query, self.__cursor_fetched = e, False
                        self.__logger.debug("execute sql:\n{}".format(e))
                        self.__cursor.execute(e)
                        self.__logger.debug("rowcount: {}".format(self.__cursor.rowcount))

    def __connect(self, e, t, fpath, encoding="cp850"):
        self.__logger.info("set source to {} ({})".format(fpath, t))
        if t == "sqlite":
            self.__connection = sqlite3.connect(fpath)
        elif t == "dbf":
            self.__connection = convert(fpath, ":memory:", logger=self.__logger, encoding=encoding)
        else:
            raise Exception ("bad kw")
        self.__cursor = self.__connection.cursor()

    def __convert(self, e, dbf_path, sqlite_path, encoding="cp850"):
        self.__connection = convert(dbf_path, sqlite_path, logger=self.__logger, encoding=encoding)
        self.__cursor = self.__connection.cursor()

    def __export(self, e, csv_path):
        self.__ensure_cursor()

        export(self.__cursor, csv_path, self.__logger)

    def __def(self, e, *args):
        import re
        from inspect import signature

        self.__logger.debug("define function python code:\n{}".format(e))
        m = re.match("^def\s+(.+)\s*\(.*$", e, re.MULTILINE)
        name = m.group(1)
        o = compile(e, "self.__script", "exec")
        exec(o)
        func = locals()[name]
        sig = signature(func)
        params = sig.parameters

        self.__connection.create_function(name, len(params), func)

    def __aggregate(self, e, *args):
        import re
        from inspect import signature

        self.__logger.debug("define aggregate function python code:\n{}".format(e))
        m = re.match("^aggregate\s+(.+)\s*\(.*$", e, re.MULTILINE)
        name = m.group(1)
        o = compile("class"+e[len("aggregate"):], "self.__script", "exec")
        exec(o)
        clazz = locals()[name]
        sig = signature(clazz.step)
        params = sig.parameters

        self.__connection.create_aggregate(name, len(params)-1, clazz)

    def __view(self, e, *args):
        self.__ensure_cursor()

        if args:
            limit = int(args[0])
        else:
            limit = 100
        view(self.__cursor, limit, self.__logger)

    def __print(self, e, *args):
        print (*args)

    def __dump(self, e, *args):
        with open(args[0], 'w', encoding="utf-8", newline="\n") as f:
            for line in self.__connection.iterdump():
                f.write(line+"\n")

    def __ensure_cursor(self):
        if self.__cursor_fetched:
            self.__cursor.execute(self.__last_query)
        self.__cursor_fetched = True

    def __get_args(self, e):
        import shlex
        return shlex.split(e)

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

def connect(dbf_path, logger=logging.getLogger("sqliteondbf"), lowernames=True, encoding="cp850", char_decode_errors="strict"):
    return convert(dbf_path, ":memory:", logger=logger, lowernames=lowernames, encoding=encoding, char_decode_errors=char_decode_errors)

def convert(dbf_path, sqlite_path, logger=logging.getLogger("sqliteondbf"), lowernames=True, encoding="cp850", char_decode_errors="strict"):
    logger.info("import {} to {}".format(dbf_path, sqlite_path))
    connection = sqlite3.connect(sqlite_path)
    _SQLiteConverter(connection, logger).import_dbf(dbf_path, encoding=encoding, lowernames=lowernames, char_decode_errors=char_decode_errors)
    return connection

def execute(script, logger=logging.getLogger("sqliteondbf")):
    _SQLiteExecutor(script, logger).execute()

def export(cursor, csv_path, logger=logging.getLogger("sqliteondbf")):
    logger.info("export data to {}".format(csv_path))
    with open(csv_path, 'w', newline='', encoding='utf-8') as dest:
        writer = csv.writer(dest)
        writer.writerow([description[0] for description in cursor.description])
        for row in cursor:
            writer.writerow(row)

def view(cursor, limit, logger=logging.getLogger("sqliteondbf")):
    logger.debug("display data on terminal")
    column_names = [description[0] for description in cursor.description]
    if limit >= 0:
        rows = [r for _, r in zip(range(limit), cursor)]
    else:
        rows = [r for r in cursor]
    ws = [max(len(str(y)) for y in col) for col in zip(column_names, *rows)]
    for zs in (column_names, *rows):
        print ("\t".join([str(z).rjust(w) if type(z) in (int, float) else str(z).ljust(w) for z, w in zip(zs, ws)]))
    if cursor.fetchone():
        print ("...")

if __name__ == '__main__':
    main()
