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
import time
from datetime import timedelta

import polars as pl
from tqdm import tqdm

# Configuration file class
from utilities.ubkg_config import ubkgConfigParser
# Centralized logging class
from utilities.ubkg_logging import ubkgLogging
# Function to find the repo root
from utilities.find_repo_root import find_repo_root
from utilities.ubkg_standardize import create_codeid, standardize_codeid, standardize_term

def get_umls_file(cfg:ubkgConfigParser, ulog:ubkgLogging, filename:str, suppress:bool = True,
                          english:bool = True, n_rows=None, cols=None, clean_file:bool=False) -> pl.DataFrame:
    """
    Returns a DataFrame corresponding to the optionally filtered content of a UMLS file.
    Uses lazy loading (pl.scan_csv and collect).

    :param suppress: if True and the file has a SUPPRESS column, suppress
    :param english: if True the file has a LAT column, filter to English
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :param filename: UMLS filename
    :param n_rows: number of rows to read
    :param cols: columns to return
    :param clean_file: if True the file will be pre-processed.
    :return: DataFrame
    """
    ufile = os.path.join(cfg.get_value(section='directories', key='umls_dir'),'META', filename+'.RRF')
    if n_rows is not None:
        rownum = str(n_rows)
    else:
        rownum = 'all'
    ulog.print_and_logger_info(f'----Reading {rownum} rows from {ufile}...')

    # Obtain the column header names from configuration.
    listcol = cfg.get_value(section='columns', key=filename).split(',')

    checksuppress = suppress and 'SUPPRESS' in listcol
    ulog.print_and_logger_info(f'----Returning only non-suppressed values: {checksuppress}')
    checkenglish = english and 'LAT' in listcol
    ulog.print_and_logger_info(f'----Returning only English-language sources: {checkenglish}')

    if clean_file:
        ulog.print_and_logger_info(f'----Pre-processing file: {ufile}...')
        cleanfile = os.path.join(cfg.get_value(section='directories', key='output_dir'), filename + '.RRF')
        # Get the total number of lines in the input file
        with open(ufile, "r", encoding="utf-8") as infile:
            total_lines = sum(1 for _ in infile)

        # Process the file with tqdm progress bar.
        with open(ufile, "r", encoding="utf-8") as infile, open(cleanfile, "w", encoding="utf-8") as outfile:
            with tqdm(total=total_lines, desc="Cleaning") as pbar:
                for line in infile:
                    # Replace improperly escaped quotes.
                    fixed_line = line.replace('"', '')
                    outfile.write(fixed_line)
                    pbar.update(1)  # Update the progress bar for each processed line
        ufile = cleanfile

    # Lazy loading and filtering.
    try:
        ldf = (pl.scan_csv(ufile,
                           separator='|',
                           new_columns=listcol,
                           n_rows=n_rows)
               .unique())
        if checksuppress:
            ldf = (ldf.filter(pl.col('SUPPRESS') != 'O'))
        if checkenglish:
            ldf = (ldf.filter(pl.col('LAT') == 'ENG'))

        # Convert LazyFrame to DataFrame with tqdm for progress.
        # Get file size in bytes.
        if n_rows is not None:
            est_total = n_rows
        else:
            file_size = os.path.getsize(ufile)
            avg_row_size = 10 * len(listcol)
            est_total = file_size//avg_row_size

        with tqdm(total=est_total, desc=f'Processing {filename}.RRF') as pbar:
            df = ldf.collect()
            pbar.update(1)

        if cols is not None:
            df = df.select(cols)

        return df

    except FileNotFoundError:
        ulog.print_and_logger_info(f'File {ufile} not found')
        exit(1)

def get_concept_code_rels(cfg:ubkgConfigParser, ulog:ubkgLogging) -> pl.DataFrame:
    """
    Builds a DataFrame of information on the relationships
    between UMLS concepts (CUIs) and codes from UMLS vocabularies.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :return: DataFrame
    """

    ulog.print_and_logger_info('Building frame of concept-code relationships...')
    ulog.print_and_logger_info('--Obtaining non-suppressed, English-language concepts...')
    # Obtain non-suppressed, English-only relationships.
    # MRCONSO.RRF contains fields that include double quotes.
    # Polars considers these as incorrectly escaped fields.
    # It is necessary to pre-process the file before reading it.
    col_conso = ['STR','SAB','CODE','TTY','CUI','AUI']
    df_mrconso = get_umls_file(cfg=cfg, ulog=ulog, filename='MRCONSO', cols=col_conso, n_rows=100, clean_file=True)

    ulog.print_and_logger_info('--Obtaining concept definitions...')
    # Obtain non-suppressed definitions.
    col_def = ['AUI','DEF']
    # MRDEF.RRF also contains fields that include double quotes, so pre-process.
    df_mrdef = get_umls_file(cfg=cfg, ulog=ulog, filename='MRDEF', cols=col_def, clean_file=True)

    # Join MRDEF data to MRCONSO data.
    df = df_mrconso.join(
        df_mrdef,
        how='left',
        on='AUI',
        maintain_order='left'
    )

    ulog.print_and_logger_info('--Standardizing codeids...')
    # Create standardized codeid.
    # Apply the transformations in steps.
    # Step 1: Create `codeid` column
    df = df.with_columns(
        create_codeid(SAB_col=pl.col('SAB'), CODE_col=pl.col('CODE'), codeid_col='codeid')
    )
    # Step 2: Standardize codeid from Step 1 to remove embedded SABs and special characters.
    df = df.with_columns(
        standardize_codeid(codeid_col='codeid')
    )

    # Add a column to terms that resemble CURIEs.
    df = df.with_columns(
        standardize_term(term_col='STR')
    )

    return df

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
    df_mrrel = get_umls_file(cfg=cfg, ulog=ulog, filename='MRREL', cols=colrels,n_rows=100)

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

def get_sources_list(cfg:ubkgConfigParser, ulog:ubkgLogging) -> list:
    """
    Builds the sources array of the JKG.JSON.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :return:
    """

    ulog.print_and_logger_info(f'Building sources list...')
    listsources = []
    return listsources

def main():

    start_time = time.time()

    #--------
    # SETUP

    # Absolute path to the root of the repo
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

    #--------
    # PRELIMINARY DATAFRAMES

    # Build concept-concept relationship DataFrame.
    # This will be used to build elements in both the nodes and
    # the rels arrays.
    df_concept_concept_rels = get_concept_concept_rels(cfg=cfg, ulog=ulog)

    # Build concept-code relationship DataFrame.
    # This will be used to build elements in both the nodes and
    # the rels arrays.
    df_concept_code_rels = get_concept_code_rels(cfg=cfg, ulog=ulog)

    # Build sources array from MRSAB:
    list_sources = get_sources_list(cfg=cfg, ulog=ulog)

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


    elapsed_time = time.time() - start_time
    ulog.print_and_logger_info(f'Completed. Total Elapsed time {"{:0>8}".format(str(timedelta(seconds=elapsed_time)))}')


if __name__ == "__main__":
    main()