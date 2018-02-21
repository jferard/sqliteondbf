# coding: utf-8
""" sqliteondbf - SQLite on DBF
      Copyright (C) 2016-2018 J. Férard <https://github.com/jferard>
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
from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sqliteondbf',
    version='0.0.1',
    description='SQLite on DBF is a very simple tool that allows to get a sqlite3 connection or to process a SQL script (sqlite flavour) on a set of dBase tables (dbf files)',
    long_description=long_description,
    url='https://github.com/jferard/sqliteondbf',
    author='Julien Férard',

    classifiers=[ 
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends',
        'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
        'Programming Language :: Python :: 3',
    ],

    keywords='sqlite dbf converter',
    install_requires=['dbfread>=2.0.7'],
    entry_points={
        'console_scripts': [
            'sqliteondbf=sqliteondbf:main',
        ],
    },
)