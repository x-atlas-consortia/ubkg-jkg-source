#!/usr/bin/env python

"""
umls2jkg.py

Converts UMLS source files into a JSON that complies with the
JSON Knowledge Graph (JKG) format.

Prerequisite:
The following files from a release of the UMLS, curated by
MetamorphoSys, are available:

MRREL.RRF
MRSAB.RRF
SRDEF (Semantic Network)
MRSTY.RRF
MRCONSO.RRF
MRDEF.RRF
MRSAT.RRF

"""

import os
import polars as pl

# Configuration file class
from utilities.ubkg_config import ubkgConfigParser
# Centralized logging class
from utilities.ubkg_logging import ubkgLogging
# Function to find the repo root
from utilities.find_repo_root import find_repo_root

def main():

    # SETUP
    # Absolute path to the root of the repo.
    repo_root = find_repo_root()

    # Centralized logging
    log_dir = os.path.join(repo_root,'log')
    log_file = 'umls2jkg.log'
    ulog = ubkgLogging(log_file=log_file,log_dir=log_dir)
    ulog.print_and_logger_info('-' * 50)
    ulog.print_and_logger_info('UMLS to JKG conversion application')

    # Configuration file
    cfg_path = os.path.join(repo_root,'src/umls2jkg.ini')
    cfg = ubkgConfigParser(path=cfg_path,log_dir=log_dir, log_file=log_file)

    # Read configuration file to obtain location of input and output
    # directories.
    umls_dir = cfg.get_value(section='directories',key='umls_dir')
    ulog.print_and_logger_info(f'umls_dir: {umls_dir}')
    output_dir = cfg.get_value(section='directories',key='output_dir')
    ulog.print_and_logger_info(f'output_dir: {output_dir}')

    # Start jkg.json file.
    # Make output directory if it does not yet exist.
    os.system(f"mkdir -p {output_dir}")

    # BUILD JSON FILE

    # Preliminary: Build concept-concept relationship DataFrame.
    # 1. Compile set of all relationships by reading MRREL and MRSAB.
    # 2. Filter out inverse relationships, possibly using the manually
    #    curated spreadsheet. Look for automated solution in umls akin to
    #    RO.json. Google: "umls find all inverse relationships" for solution
    #    using MRDOC.DEF.

    # Preliminary: Build concept-code relationship DataFrame,
    # querying MRCONSO and MRDEF.

    # Build sources array from MRSAB:
    # 1. Read configuration file to obtain source information for UMLS.
    # 2. Add UMLS SAB to sources array.

    # Build nodes array:
    # 1. Obtain Node_Label nodes from Semantic Network (SRDEF).
    # 2. Append Rel_Label nodes from the concept-concept relationships frame.
    # 3. Append Concept nodes, using MRCONSO.
    # 4. Append Term nodes, using the concept-code relationship dataframe to identify
    #    codes.

    # Build rels array:
    # 1. Semantic relationships
    # 2. Concept-concept relationships
    # 3. Concept-code relationships
    # 4. Add maps of NDC codes to CUIs to rels


if __name__ == "__main__":
    main()