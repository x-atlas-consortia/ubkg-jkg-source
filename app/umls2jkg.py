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
from app.classes.umls_reader import UmlsReader
# Function to find the repo root
from utilities.find_repo_root import find_repo_root
from utilities.ubkg_standardize import create_codeid, standardize_codeid, standardize_term

def get_source_node_list(cfg:UbkgConfigParser, ulog:UbkgLogging) -> list:
    """
    Builds the list of Source nodes for the nodes array of the JKG.JSON.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :return:
    """

    #Obtain sorted current English-language SABs from MRSAB.RRF.
    colsabs = ['VSAB', 'RSAB', 'SON', 'SRL', 'TTYL']
    # VSAB - versioned source
    # RSAB - root source
    # SON - official name of source
    # SRL - source restriction level - can be used to filter out a licensed SAB
    # TTYL - term types from the SAB
    df = get_umls_file(cfg=cfg, ulog=ulog, filename='MRSAB', cols=colsabs)
    df = df.sort('RSAB')

    # Convert to a list of dictionaries for row-wise processing
    rows = df.to_dicts()

    # Build JSON output row by row.
    # Start with hard-coded rows for:
    # - the UMLS itself
    # - NDC

    listsources = [{"labels":["Source"],"properties":{"id":"UMLS:UMLS","name":"Unified Medical Language System","description":"United States National Institutes of Health (NIH) National Library of Medicine (NLM) Unified Medical Language System (UMLS) Knowledge Sources.","sab":"UMLS" ,"source":"http://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html"}}]
    listsources.append({"labels":["Source"] ,"properties":{"id":"UMLS:NDC" ,"name":"National Drug Codes","sab":"NDC"}})

    for row in tqdm(rows, desc="Building Source nodes"):
        json_str = json.dumps({
            "labels": ["Source"],
            "properties": {
                "id": f"UMLS:{row['VSAB']}",
                "name": row["SON"],
                "sab": row["RSAB"],
                "srl": row["SRL"],
                "ttyl": row["TTYL"].split(",") if row["TTYL"] else []  # Convert TTYL to a list or an empty list
            }
        })
        listsources.append(json_str)

    return listsources

def get_semantic_node_label_list(cfg:UbkgConfigParser, ulog:UbkgLogging) -> list:
    """
    Builds the list of Node_Label nodes corresponding to the
    Semantic Network for the nodes array of the JKG.JSON.

    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :return:
    """

    list_nodes = []

    # Obtain information for the Semantic Network nodes from SRDEF.
    colsem = ['RT', 'UI', 'STY_RL', 'DEF']
    # RT - Record type: STY = semantic
    # UI - Unique identifier
    # STY_RL - name of the semantic relation
    # DEF - definition
    df = get_umls_file(cfg=cfg, ulog=ulog, filename='SRDEF', cols=colsem)
    df = df.filter(pl.col('RT')=='STY')

    # Convert to a list of dictionaries for row-wise processing
    rows = df.to_dicts()

    # Build JSON output row by row.
    for row in tqdm(rows, desc="Building Semantic Network Node_Label nodes"):
        json_str = json.dumps({
            "labels": ["Node_Label"],
            "properties": {
                "id": f"UMLS:{row['UI']}",
                "def": row["DEF"],
                "node_label": row["STY_RL"],
                "sab": "UMLS"}
        })
        list_nodes.append(json_str)

    return list_nodes

def get_rel_nodes_list(df:pl.DataFrame) -> list:
    """
    Builds the list of Rel_Label nodes for the nodes array of the JKG.JSON.
    :param df: DataFrame of concept-concept relationships
    :return:
    """
    list_nodes = []

    # Get the relationship labels.
    df = df.select('rel_label').unique().sort('rel_label')
    rows = df.to_dicts()

    for row in tqdm(rows, desc="Bulding Rel_Label nodes from concept-concept relationships"):
        json_str = json.dumps({
            "labels": ["Rel_Label"],
            "properties": {
                "id": f"UMLS:{row["rel_label"]}",
                "def": row["rel_label"],
                "rel_label": row["rel_label"],
                "sab": "UMLS"}
        })
        list_nodes.append(json_str)

    return list_nodes

def get_concept_labels(cfg:UbkgConfigParser, ulog:UbkgLogging) -> pl.DataFrame:
    """
    Obtains a DataFrame that aggregates the semantic types for each CUI.
    :param cfg: UbkgConfigParser instance
    :return: DataFrame
    """

    # Get semantic atoms (terms) by concept.
    colsem=['CUI','STY']
    # CUI
    # STY - semantic type
    df = get_umls_file(cfg=cfg, ulog=ulog, filename='MRSTY',cols=colsem)

    # normalize empty STY to null so they don't become empty strings in the lists
    df = df.with_columns(
        pl.when(pl.col("STY") == "").then(None).otherwise(pl.col("STY")).alias("STY")
    )

    # Group semantic type labels by CUI; aggregate into lists; then append "Concept" to each list.
    dfsty = (
        df.group_by("CUI", maintain_order=True)
        .agg(pl.col("STY"))
        .with_columns((pl.concat_list(pl.lit("Concept"),"STY")).alias("labels"))
    )

    return dfsty

def get_concept_nodes_list(cfg:UbkgConfigParser, ulog:UbkgLogging, df:pl.DataFrame) -> list:
    """
    Builds the list of Concept_Label nodes of the nodes array of the JKG.JSON.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :param df: DataFrame of concept-concept relationships
    :return:
    """
    list_nodes = []

    # Identify the preferred term for each concept from the
    # record in the English-language, non-suppressed subset of
    # MRCONSO for which
    #   -- the Atom Status (TS) is preferred  (ISPREF=Y)
    #   -- the String type (STT) is "PF" (Preferred form of term)
    #   -- the Term Status is "P" (Preferred LUI of the CUI)
    df = (df.filter(pl.col('ISPREF')=='Y')
          .filter(pl.col('STT')=='PF')
          .filter(pl.col('TS')=='P')
          .unique())

    # Obtain sorted list of concept labels for each concept.
    dflabels = get_concept_labels(cfg=cfg, ulog=ulog)

    df = df.join(dflabels,
                 how='inner',
                 on='CUI')

    rows = df.to_dicts()

    for row in tqdm(rows, desc="Building Concept nodes"):
        json_str = json.dumps({
            "labels": row["labels"],
            "properties": {
                "id": f"UMLS:{row["CUI"]}",
                "pref_term": row["STR"],
                "sab": "UMLS"}
        })
        list_nodes.append(json_str)

    return list_nodes


def get_nodes_list(cfg:UbkgConfigParser, ulog:UbkgLogging,
                   df_concept_concept_rels:pl.DataFrame,
                   df_concept_code_rels:pl.DataFrame) -> list:
    """
    Builds the list of nodes array of the JKG.JSON.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :param df_concept_concept_rels: DataFrame of concept-concept relationships
    :param df_concept_code_rels: DataFrame of concept-code relationships
    :return:
    """

    # Build nodes array:
    # 1. Obtain Node_Label nodes from Semantic Network (SRDEF).
    # 2. Append Rel_Label nodes from the concept-concept relationships DataFrame.
    # 3. Append Concept nodes from the concept-code relationships DataFrame.
    # 4. Append Term nodes, using the concept-code relationship Dataframe.

    ulog.print_and_logger_info('Building nodes list...')
    list_nodes = (get_source_node_list(cfg=cfg, ulog=ulog) +
                  get_semantic_node_label_list(cfg=cfg, ulog=ulog) +
                  get_rel_nodes_list(df=df_concept_concept_rels) +
                  get_concept_nodes_list(cfg=cfg, ulog=ulog, df=df_concept_code_rels))

    return list_nodes

def get_rels_list(cfg:UbkgConfigParser, ulog:UbkgLogging) -> list:
    """
    Builds the list of relations array of the JKG.JSON.
    :param cfg: UbkgConfigParser instance
    :param ulog: UbkgLogging instance
    :param df: DataFrame of concept-code relationships
    :return:
    """

    ulog.print_and_logger_info('Building rels list...')
    list_nodes = []

    return list_nodes


def main():

    start_time = time.time()

    #--------
    # SETUP

    # Absolute path to the root of the repo
    repo_root = find_repo_root()

    # Centralized logging object
    log_dir = os.path.join(repo_root,'app/log')
    log_file = 'umls2jkg.log'
    ulog = UbkgLogging(log_file=log_file,log_dir=log_dir)
    ulog.print_and_logger_info('-' * 50)
    ulog.print_and_logger_info('UMLS TO JKG CONVERSION')
    ulog.print_and_logger_info('-' * 50)

    # Configuration file object
    cfg_path = os.path.join(repo_root,'app/umls2jkg.ini')
    cfg = UbkgConfigParser(path=cfg_path,log_dir=log_dir, log_file=log_file)

    # UMLS reader object
    uread = UmlsReader(cfg=cfg, ulog=ulog)

    # Read configuration file to obtain location of output directory.
    output_dir = cfg.get_value(section='directories',key='output_dir')
    ulog.print_and_logger_info(f'Output directory: {output_dir}')
    ulog.print_and_logger_info('-' * 25)


    # Start jkg.json file.
    # Make output directory if it does not yet exist.
    os.system(f"mkdir -p {output_dir}")

    #list_nodes = get_nodes_list(cfg=cfg, ulog=ulog,
                               # df_concept_concept_rels=df_concept_concept_rels,
                                #df_concept_code_rels=df_concept_code_rels)

    #list_nodes =  get_concept_nodes_list(cfg=cfg, ulog=ulog, df=df_concept_code_rels)

    # Build rels array:
    # 1. Semantic relationships
    # 2. Concept-concept relationships
    # 3. Concept-code relationships
    # 4. Add maps of NDC codes to CUIs to rels


    elapsed_time = time.time() - start_time
    ulog.print_and_logger_info(f'Completed. Total Elapsed time {"{:0>8}".format(str(timedelta(seconds=elapsed_time)))}')


if __name__ == "__main__":
    main()