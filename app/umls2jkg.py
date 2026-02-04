#!/usr/bin/env python

"""
umls2jkg.py

Converts UMLS source files into a JSON that complies with the
JSON Knowledge Graph (JKG) format.

Prerequisite:
A subset of UMLS files built from a release of the UMLS, curated by
MetamorphoSys. Files include:

MRREL.RRF (relationships)
MRSAB.RRF (sources)
SRDEF (semantic type descriptions from the Semantic Network)
MRSTY.RRF (concept semantic types)
MRCONSO.RRF (concepts codes)
MRDEF.RRF (concept definitions)
MRSAT.RRF (concept attributes)

"""

import os
import time

from datetime import timedelta

import polars as pl
import json
from tqdm import tqdm

# Configuration file class
from app.classes.ubkg_config import UbkgConfigParser
# Centralized logging class
from app.classes.ubkg_logging import UbkgLogging

# Class that writes to the JKG JSON
from app.classes.jkg_writer import JkgWriter

# Function to find the repo root
from utilities.find_repo_root import find_repo_root



def main():

    start_time = time.time()

    #--------
    # SETUP

    # Get absolute path to the root of the repo
    repo_root = find_repo_root()

    # Set up objects.

    # Centralized logging object
    log_dir = os.path.join(repo_root,'app/log')
    log_file = 'umls2jkg.log'
    ulog = UbkgLogging(log_file=log_file,log_dir=log_dir)

    # Configuration file object
    cfg_path = os.path.join(repo_root,'app/umls2jkg.ini')
    cfg = UbkgConfigParser(path=cfg_path,log_dir=log_dir, log_file=log_file)

    # JKG writer object
    jwriter = JkgWriter(cfg=cfg, ulog=ulog)

    ulog.print_and_logger_info('-' * 50)
    ulog.print_and_logger_info('UMLS TO JKG CONVERSION')
    ulog.print_and_logger_info('-' * 50)

    # Build and write the nodes list.
    jwriter.write_nodes_list()

    # Build and write the rels list.
    jwriter.write_rels_list()

    elapsed_time = time.time() - start_time
    ulog.print_and_logger_info(f'Completed. Total Elapsed time {"{:0>8}".format(str(timedelta(seconds=elapsed_time)))}')


if __name__ == "__main__":
    main()