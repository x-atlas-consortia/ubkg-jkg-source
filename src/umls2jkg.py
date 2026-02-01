#!/usr/bin/env python

"""
umls2jkg.py

Converts UMLS source files into a JSON that complies with the
JSON Knowledge Graph (JKG) format.

Prerequisite:
A subset of UMLS files built from a release of the UMLS, curated by
MetamorphoSys. Files include:

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
from tqdm import tqdm

# Configuration file class
from utilities.ubkg_config import ubkgConfigParser
# Centralized logging class
from utilities.ubkg_logging import ubkgLogging
# Function to find the repo root
from utilities.find_repo_root import find_repo_root

def get_umls_file(cfg:ubkgConfigParser, ulog:ubkgLogging, filename:str, suppress:bool = True,
                          english:bool = True, n_rows=None, cols=None) -> pl.DataFrame:
    """
    Returns a DataFrame corresponding to the optionally filtered content of a UMLS file

    :param suppress: if True and the file has a SUPPRESS column, suppress
    :param english: if True the file has a LAT column, filter to English
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :param filename: UMLS filename
    :param n_rows: number of rows to read
    :param cols: columns to return
    :return: DataFrame
    """
    ufile = os.path.join(cfg.get_value(section='directories', key='umls_dir'),'META', filename+'.RRF')
    ulog.print_and_logger_info(f'Reading file {ufile}...')

    # Obtain the column header names from configuration.
    listcol = cfg.get_value(section='columns', key=filename).split(',')

    checksuppress = suppress and 'SUPPRESS' in listcol
    checkenglish = english and 'LAT' in listcol

    # Use lazy read (scan_csv) if possible.
    try:
        if checksuppress and checkenglish:
            df = (pl.scan_csv(ufile, separator='|', new_columns=listcol, n_rows=n_rows)
                  .filter(pl.col('SUPPRESS') != 'O')
                  .filter(pl.col('LAT') == 'ENG')
                  .unique())
        elif checksuppress:
            df = (pl.scan_csv(ufile, separator='|', new_columns=listcol, n_rows=n_rows)
                  .filter(pl.col('SUPPRESS') != 'O')
                  .unique())
        elif checkenglish:
            df = (pl.scan_csv(ufile, separator='|', new_columns=listcol, n_rows=n_rows)
                  .filter(pl.col('LAT') == 'ENG')
                  .unique())
        else:
            df = (pl.scan_csv(ufile, separator='|', new_columns=listcol, n_rows=n_rows)
                  .unique())

        # Convert LazyFrame to DataFrame with tqdm for progress
        with tqdm(total=1, desc=f'Processing {filename}.RRF') as pbar:
            df = df.collect()  # Materialize the LazyFrame into memory
            pbar.update(1)

        if cols is not None:
            df = df.select(cols)

        return df

    except FileNotFoundError:
        ulog.print_and_logger_info(f'File {ufile} not found')
        exit(1)

def get_concept_concept_rels(cfg:ubkgConfigParser, ulog:ubkgLogging) -> pl.DataFrame:
    """
    Builds a DataFrame of information on the relationships
    between UMLS concepts.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :return: DataFrame
    """

    ulog.print_and_logger_info(f'Building frame of concept-concept relationships...')
    ulog.print_and_logger_info(f'--Obtaining non-suppressed relationships...')
    # Obtain non-suppressed relationships.
    colrels = ['CUI1','CUI2','REL','RELA','SAB']
    df_mrrel = get_umls_file(cfg=cfg, ulog=ulog, filename='MRREL', cols=colrels)

    # Filter to English-language SABs.
    colsabs = ['RSAB','LAT']
    df_mrsab = get_umls_file(cfg=cfg, ulog=ulog, filename='MRSAB',cols=colsabs)
    # Filter to relationships defined in English-language SABs.
    ulog.print_and_logger_info(f'--Filtering to relationships from English-language SABs...')
    df_rel = (
        df_mrrel.join(
            df_mrsab,
            how='inner',
            left_on='SAB',
            right_on='RSAB',
            maintain_order='left')
        .unique())

    # Filter out inverse relationships.
    ulog.print_and_logger_info(f'--Selecting the forward relationships of forward/inverse relationship pairs...')

    # For forward/inverse relationship pairs, MRDOC contains two records, with each relationship
    # in the pair in both the VALUE and EXPL columns.
    # e.g.,
    # DOCKEY | VALUE       | TYPE          | EXPL         |
    # RELA | nerve_supply_of | rela_inverse | has_nerve_supply |
    # RELA | has_nerve_supply | rela_inverse | nerve_supply_of |

    # Resolve the paired rows and select as the "forward relationship"
    # the relationship for which the value is the last alphabetically
    # in the pair.

    df_inverse_rel_pairs = (get_umls_file(cfg=cfg, ulog=ulog, filename='MRDOC')
                .filter(pl.col('DOCKEY') == 'RELA')
                .filter(pl.col('TYPE') == 'rela_inverse'))

    df_forward = (
        df_inverse_rel_pairs
        .with_columns(
            # Create an identifier for pairs, considering VALUE and EXPL as reciprocal links.
            pl.when(pl.col("VALUE") < pl.col("EXPL"))
            .then(pl.col("VALUE") + "~" + pl.col("EXPL"))
            .otherwise(pl.col("EXPL") + "~" + pl.col("VALUE"))
            .alias("pair_group")
        )
        .group_by("pair_group")  # Group on the pair identifier
        .agg([
            # Retain only the row with the alphabetically last VALUE
            pl.col("VALUE").sort().last().alias("VALUE"),
            pl.col("DOCKEY").last(),
            pl.col("TYPE").last(),
            pl.col("EXPL").last(),
        ])
    )

    df_inverse = (
        df_inverse_rel_pairs
        .with_columns(
            # Create an identifier for pairs, considering VALUE and EXPL as reciprocal links.
            pl.when(pl.col("VALUE") < pl.col("EXPL"))
            .then(pl.col("VALUE") + "~" + pl.col("EXPL"))
            .otherwise(pl.col("EXPL") + "~" + pl.col("VALUE"))
            .alias("pair_group")
        )
        .group_by("pair_group")  # Group on the pair identifier
        .agg([
            # Retain only the row with the alphabetically last VALUE
            pl.col("VALUE").sort().first().alias("VALUE")
        ])
    )

    df_rel = df_rel.join(
        df_forward,
        how='left',
        left_on='RELA',
        right_on='VALUE',
    ).unique().select(colrels)

    # Print out the inverse relationships for comparison with the
    # manually-curated version.
    out_file = os.path.join(cfg.get_value(section='directories',key='output_dir'),'inverse_relationships.csv')
    df_inverse.select(pl.col('VALUE')).sort('VALUE').write_csv(out_file)

    return df_rel

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

    # Read configuration file to obtain location of output directory.
    output_dir = cfg.get_value(section='directories',key='output_dir')
    ulog.print_and_logger_info(f'output_dir: {output_dir}')

    # Start jkg.json file.
    # Make output directory if it does not yet exist.
    os.system(f"mkdir -p {output_dir}")

    # BUILD JSON FILE

    # Preliminary: Build concept-concept relationship DataFrame.
    df_concept_concept_rels = get_concept_concept_rels(cfg=cfg, ulog=ulog)

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