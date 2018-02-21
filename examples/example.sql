/* sqliteondbf - SQLite on DBF
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
*/

-- Note: The example files are adapted from https://www.census.gov/data/tables/2016/econ/stc/2016-annual.html (I didn't find a copyright, but this is fair use I believe)

$connect dbf "." "utf-8"

$def my_uppercase(v):
    return v.upper()

DROP VIEW IF EXISTS detailed

CREATE VIEW detailed AS
SELECT
    d.state_code,
    my_uppercase(s.state_name) as state_name,
    d.survey_yea,
    d.item_code,
    my_uppercase(i.item_name) as item_name,
    d.amount
FROM 
    "2016-stc-detailed" d 
    LEFT JOIN state s 
    ON d.state_code = s.state_code
    LEFT JOIN item i 
    ON d.item_code = i.item_code

SELECT * FROM detailed
ORDER BY CAST(state_code as INT), CAST(item_code as INT)

$export "detailed.csv"

SELECT 
    state_code,
    my_uppercase(state_name),
    survey_yea,
    SUM(amount) as amount
FROM detailed
GROUP BY state_code, my_uppercase(state_name), survey_yea
ORDER BY SUM(amount) DESC

$export "by_state.csv"

DROP VIEW IF EXISTS detailed