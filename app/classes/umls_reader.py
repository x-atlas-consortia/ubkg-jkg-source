#!/usr/bin/env python
# coding: utf-8
"""
Class that reads and translates data from the flat source files
of a subset produced by the UMLS MetamorphoSYS application.
"""

import os
import time

import polars as pl
import json
from tqdm import tqdm

# Configuration file class
from app.classes.ubkg_config import UbkgConfigParser
# Centralized logging class
from app.classes.ubkg_logging import UbkgLogging

# Functions to standardize codes and terms from vocabularies
from app.utilities.ubkg_standardize import create_codeid, standardize_codeid, standardize_term

class UmlsReader:

    def __init__(self, cfg: UbkgConfigParser, ulog: UbkgLogging):

        self.cfg = cfg
        self.ulog = ulog

        message = 'NOTE: The processing time to read a 6 GB MRREL.RRF on 32 GB RAM MacBook is ~30s. The other files take significantly less time.'
        print(f"\033[32m{message}\033[0m")

        # Build Dataframes that will be used to build elements in
        # both the nodes and the rels arrays in JKG.JSON.

        # Concept-concept relationships
        self.df_concept_concept_rels = self._get_concept_concept_rels()

        # Concept-code relationships
        self.df_concept_code_rels = self._get_concept_code_rels()

        # Semantic definitions
        self.df_semantic_definitions = self._get_semantic_definitions()

    def get_umls_file(self,filename: str, suppress: bool = True,
                      english: bool = True, curver: bool = True, n_rows=None, cols=None,
                      clean_file: bool = False) -> pl.DataFrame:
        """
        Returns a DataFrame corresponding to the optionally filtered content of a UMLS file.
        Uses lazy loading (pl.scan_csv and collect).

        :param suppress: if True and the file has a SUPPRESS (Suppressible) column, suppress
        :param english: if True and the file has a LAT (Language) column, filter to English
        :param curver: if True the file has a CURVER (Current Version) column, filter to current version
        :param filename: UMLS filename
        :param n_rows: number of rows to read
        :param cols: columns to return
        :param clean_file: if True the file will be pre-processed to remove double-quoted strings
        """

        # The Semantic Network definitions files and Metathesaurus files
        # are located in separate directories.
        if filename[:2] == 'SR':
            ufile = os.path.join(self.cfg.get_value(section='directories', key='umls_dir'), 'NET', filename)
        else:
            ufile = os.path.join(self.cfg.get_value(section='directories', key='umls_dir'), 'META', filename + '.RRF')

        # Estimate number of rows to be processed.
        if n_rows is not None:
            est_total = n_rows
        else:
            file_size = os.path.getsize(ufile)
            # Obtain the sample row size for the file from configuration.
            avg_row_size = int(self.cfg.get_value(section='rowsizes', key=filename))
            est_total = int(round(file_size / avg_row_size, 0))
        rownum = str(est_total)

        # Obtain the column header names from configuration.
        listcol = self.cfg.get_value(section='columns', key=filename).split(',')
        if cols is None:
            cols = listcol

        checksuppress = suppress and 'SUPPRESS' in listcol
        checkenglish = english and 'LAT' in listcol
        checkcurver = curver and 'CURVER' in listcol

        if clean_file:
            # Pre-process the source file if one does not already exist.
            ufile = self._get_clean_file(filename=filename)

        # Lazy loading and filtering.
        try:
            start_time = time.time()

            ldf = (pl.scan_csv(ufile,
                               separator='|',
                               new_columns=listcol,
                               n_rows=n_rows)
                   .unique())
            if checksuppress:
                ldf = (ldf.filter(pl.col('SUPPRESS') != 'O'))
            if checkenglish:
                ldf = (ldf.filter(pl.col('LAT') == 'ENG'))
            if checkcurver:
                ldf = (ldf.filter(pl.col('CURVER') == 'Y'))

            # Convert LazyFrame to DataFrame with tqdm for progress.

            with tqdm(total=est_total, desc=f'Processing {filename}.RRF', unit='row') as pbar:
                df = ldf.collect()
                pbar.update(1)

            if cols is not None:
                df = df.select(cols)

            end_time = time.time()
            duration = end_time - start_time
            self.ulog.print_and_logger_info(
                message=f'----Processed ~{rownum} rows from {filename} in {duration:.2f} seconds.')
            return df

        except FileNotFoundError:
            self.ulog.print_and_logger_info(f'File {ufile} not found')
            exit(1)

    def _get_clean_file(self, filename: str) -> str:
        """
        Pre-processes a UMLS file to remove double-quoted strings.
        :param filename: UMLS filename

        :return: the path to the cleaned file.

        Assumes that the file has RRF as an extension.
        """

        dirtyfile = os.path.join(self.cfg.get_value(section='directories', key='umls_dir'), 'META', filename + '.RRF')
        cleanfile = os.path.join(self.cfg.get_value(section='directories', key='output_dir'), filename + '.RRF')

        if os.path.exists(cleanfile):
            self.ulog.print_and_logger_warning(
                f'----Using existing cleaned file {cleanfile}. Delete this file to force pre-processing.')
        else:
            self.ulog.print_and_logger_info(f'----Cleaning file: {dirtyfile}...')

            # Get the total number of lines in the input file
            with open(dirtyfile, "r", encoding="utf-8") as infile:
                total_lines = sum(1 for _ in infile)

            # Process the file with tqdm progress bar.
            with open(dirtyfile, "r", encoding="utf-8") as infile, open(cleanfile, "w", encoding="utf-8") as outfile:
                with tqdm(total=total_lines, desc="Cleaning") as pbar:
                    for line in infile:
                        # Replace improperly escaped quotes.
                        fixed_line = line.replace('"', '')
                        outfile.write(fixed_line)
                        pbar.update(1)  # Update the progress bar for each processed line

        return cleanfile

    def _get_forward_relationships(self) -> pl.DataFrame:
        """
        Builds a dataframe of information on "forward" relationships, defined
        to be the relationship in a bilateral pair of relationships with
        term that is alphabetically last.

        For example, for the bilateral pair has_nerve_supply/nerve_supply_of,
        "nerve_supply_of" is considered the forward relationship.

        :param cfg: UbkgConfigParser instance
        :param ulog: UbkgLogging instance

        """

        """
        For forward/inverse relationship pairs, MRDOC contains two records, with each relationship
        in the pair in both the VALUE and EXPL columns.

        Example:
        DOCKEY | VALUE       | TYPE          | EXPL         |
        RELA | nerve_supply_of | rela_inverse | has_nerve_supply |
        RELA | has_nerve_supply | rela_inverse | nerve_supply_of |

        Resolve the paired rows and select as the "forward relationship"
        the relationship for which the value is the last alphabetically
        in the pair.
        """

        df_inverse_rel_pairs = (self.get_umls_file(filename='MRDOC')
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

        # Print out the inverse relationships for comparison with the
        # manually-curated version.
        out_file = os.path.join(self.cfg.get_value(section='directories', key='output_dir'), 'inverse_relationships.csv')
        # ulog.print_and_logger_info(f'(List of filtered inverse relationships at {out_file})')
        df_inverse.select(pl.col('VALUE')).sort('VALUE').write_csv(out_file)

        return df_forward

    def _get_concept_concept_rels(self) -> pl.DataFrame:
        """
        Builds a DataFrame of information on the relationships
        between UMLS concepts.
        """
        print('')
        self.ulog.print_and_logger_info(f'Building information on concept-concept relationships...')

        # Obtain non-suppressed relationships from MRREL.RRF.
        colrels = ['CUI1', 'CUI2', 'REL', 'RELA', 'SAB']
        # CUI1 - CUI of start concept
        # CUI2 - CUI of end concept
        # REL - required id for relationship (usually 2 character)
        # RELA - optional, more specific description (usually delimited)
        # SAB - source of relationship

        df_mrrel = self.get_umls_file(filename='MRREL', cols=colrels,n_rows=1000)
        # Get the relationship label--the value of RELA if not null else
        # the value of REL.
        df_mrrel = df_mrrel.with_columns(
            pl.when(pl.col("RELA").is_null())
            .then(pl.col("REL"))
            .otherwise(pl.col("RELA"))
            .alias("rel_label")
        )
        colrels.append('rel_label')

        # Filter to English-language SABs.
        # ulog.print_and_logger_info(f'--Filtering to relationships from English-language SABs using MRSAB.RRF...')
        colsabs = ['RSAB', 'LAT']
        # RSAB - SAB acronym
        # LAT - language
        df_mrsab = self.get_umls_file(filename='MRSAB', cols=colsabs)
        # Filter to relationships defined in English-language SABs.
        df_rel = (
            df_mrrel.join(
                df_mrsab,
                how='inner',
                left_on='SAB',
                right_on='RSAB',
                maintain_order='left')
            .unique())

        # Filter out inverse relationships.
        df_forward = self._get_forward_relationships()

        df_rel = df_rel.join(
            df_forward,
            how='left',
            left_on='RELA',
            right_on='VALUE',
        ).unique().select(colrels)

        return df_rel

    def _get_concept_code_rels(self) -> pl.DataFrame:
        """
        Builds a DataFrame of information on the relationships
        between UMLS concepts (CUIs) and codes from UMLS vocabularies.
        """

        print('')
        self.ulog.print_and_logger_info('Building data for concept-code relationships...')

        # Obtain non-suppressed, English-only relationships.
        # MRCONSO.RRF contains fields that include double quotes.
        # Polars considers these as incorrectly escaped fields.
        # It is necessary to pre-process the file before reading it.

        col_conso = ['STR', 'SAB', 'CODE', 'TTY', 'CUI', 'AUI', 'ISPREF', 'STT', 'TS']
        # MRCONSO contains one row per atom -- i.e., unique combination of CUI/code/term
        # AUI - atom identifier, equivalent to primary key
        # CUI - CUI of concept
        # SAB - SAB of source of code linked to concept
        # CODE - ID of code in SAB
        # TTY - term type of term
        # STR - term string
        # STT - string type--in particular, 'PF' means preferred term
        # ISPREF - whether the term is the preferred for the concept
        # TS - term status

        df_mrconso = self.get_umls_file(filename='MRCONSO', cols=col_conso, clean_file=True)

        # Obtain non-suppressed definitions.
        col_def = ['AUI', 'DEF']
        # AUI - identifier
        # DEF - definition string
        # MRDEF.RRF also contains fields that include double quotes, so pre-process.
        df_mrdef = self.get_umls_file(filename='MRDEF', cols=col_def, clean_file=True)

        # Join MRDEF data to MRCONSO data.
        df = df_mrconso.join(
            df_mrdef,
            how='left',
            on='AUI',
            maintain_order='left'
        )

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

    def _get_semantic_definitions(self) -> pl.DataFrame:
        """
        Builds a DataFrame of information on semantic definitions from SRDEF

        """

        # Obtain information for the Semantic Network nodes from SRDEF.
        colsem = ['RT', 'UI', 'STY_RL', 'DEF']
        # RT - Record type: STY = semantic
        # UI - Unique identifier
        # STY_RL - name of the semantic relation
        # DEF - definition
        df = self.get_umls_file(filename='SRDEF', cols=colsem)

        return df


