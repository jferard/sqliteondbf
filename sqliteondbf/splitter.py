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

class SemicolonSplitter():
    __NONE = 0

    __OPEN_BLOCK_COMMENT_C1 = 10
    __BLOCK_COMMENT_OPENED = 11
    __BLOCK_COMMENT_OPENED_CLOSE_BLOCK_COMMENT_C1 = 12

    __OPEN_LINE_COMMENT_C1 = 20
    __OPEN_LINE_COMMENT_C1 = 21
    __LINE_COMMENT_OPENED = 22

    __DOUBLE_QUOTED = 30
    __DOUBLE_QUOTED_ESCAPE = 31

    __SINGLE_QUOTED = 40
    __SINGLE_QUOTED_ESCAPE = 41

    def __init__(self, block_comment=("/*", "*/"), line_comment="--", splitter=";"):
        self.__block_comment = block_comment
        self.__line_comment = line_comment
        self.__splitter = splitter

    def split(self, script):
        cur_chunk = []
        state = SemicolonSplitter.__NONE
        for c in script:
            if state == SemicolonSplitter.__NONE:
                if c == self.__block_comment[0][0]:
                    state = SemicolonSplitter.__OPEN_BLOCK_COMMENT_C1
                elif c == self.__line_comment[0]:
                    state = SemicolonSplitter.__OPEN_LINE_COMMENT_C1
                elif c == "\"":
                    state = SemicolonSplitter.__DOUBLE_QUOTED
                elif c == "'":
                    state = SemicolonSplitter.__SINGLE_QUOTED
                elif c == self.__splitter:
                    chunk = "".join(cur_chunk).strip()
                    if chunk:
                        yield chunk
                    cur_chunk = []
                    continue
            elif state == SemicolonSplitter.__OPEN_BLOCK_COMMENT_C1:
                if c == self.__block_comment[0][1]:
                    state = SemicolonSplitter.__BLOCK_COMMENT_OPENED
                else:
                    state = SemicolonSplitter.__NONE
            elif state == SemicolonSplitter.__BLOCK_COMMENT_OPENED:
                if c == self.__block_comment[1][0]:
                    state = SemicolonSplitter.__BLOCK_COMMENT_OPENED_CLOSE_BLOCK_COMMENT_C1
            elif state == SemicolonSplitter.__BLOCK_COMMENT_OPENED_CLOSE_BLOCK_COMMENT_C1:
                if c == self.__block_comment[1][1]:
                    cur_chunk = []
                    state = SemicolonSplitter.__NONE
                    continue
            elif state == SemicolonSplitter.__OPEN_LINE_COMMENT_C1:
                if c == self.__line_comment[1]:
                    state = SemicolonSplitter.__LINE_COMMENT_OPENED
                else:
                    state = SemicolonSplitter.__NONE
            elif state == SemicolonSplitter.__LINE_COMMENT_OPENED:
                if c == "\n":
                    cur_chunk = []
                    state = SemicolonSplitter.__NONE
                    continue
            elif state == SemicolonSplitter.__DOUBLE_QUOTED:
                if c == "\\":
                    state = SemicolonSplitter.__DOUBLE_QUOTED_ESCAPE
                if c == "\"":
                    state = SemicolonSplitter.__NONE
            elif state == SemicolonSplitter.__DOUBLE_QUOTED_ESCAPE:
                state = SemicolonSplitter.__DOUBLE_QUOTED;
            elif state == SemicolonSplitter.__SINGLE_QUOTED:
                if c == "\\":
                    state = SemicolonSplitter.__SINGLE_QUOTED_ESCAPE
                if c == "'":
                    state = SemicolonSplitter.__NONE
            elif state == SemicolonSplitter.__SINGLE_QUOTED_ESCAPE:
                state = SemicolonSplitter.__SINGLE_QUOTED;

            cur_chunk.append(c)

        chunk = "".join(cur_chunk).strip()
        if chunk:
            yield chunk
