#!/usr/bin/env python
"""mapper.py"""

import sys

DATE = 7

for line in sys.stdin:
    line = line.strip()

    components = line.split(',')

    if '2008-01-01' <= components[DATE] <= '2018-12-31':
        print(line)
