#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@author: rwang
@file: logger.py
@time: 2021/1/31 20:03
"""
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

TS = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

log_dir = "logs/"

# init logger
os.makedirs(log_dir, exist_ok=True)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('[%(asctime)s] %(module)s.%(funcName)s - %(levelname)s: %(message)s')
fh = logging.FileHandler(filename=os.path.join(log_dir, "strategy_lab_{}.log").format(TS))
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(ch)
