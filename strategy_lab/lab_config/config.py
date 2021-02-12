#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: config.py
@time: 2021/2/4 18:02
"""
import os
from datetime import datetime
from os.path import join

from config.config import PROJECT_ROOT

OUT_DIR = join(PROJECT_ROOT, "local_scripts/lab_output")

TRADE_DAY_NUM_OF_YEAR = 252
TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
DATE_FMT = "%Y-%m-%d"

ZERO_DATE = "1990-01-01"
LAST_DATE = "2050-01-01"
TODAY = datetime.now().strftime(DATE_FMT)
