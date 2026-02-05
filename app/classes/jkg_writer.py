#!/usr/bin/env python
# coding: utf-8
"""
Class that builds a JSON that conforms to the
JSON Knowledge Graph (JKG) schema.
"""

import os

import polars as pl
import json
import textwrap
from tqdm import tqdm

# Configuration file class
from app.classes.ubkg_config import UbkgConfigParser
# Centralized logging class
from app.classes.ubkg_logging import UbkgLogging
# Class that reads and prepares data from UMLS flat files
from app.classes.umls_reader import UmlsReader
# Class that writes to JSON output
from app.classes.json_writer import JsonWriter

class JkgWriter:

    def __init__(self, cfg:UbkgConfigParser, ulog:UbkgLogging):
        self.cfg = cfg
        self.ulog = ulog

        # Read configuration file to obtain location of output directory.
        self.output_dir = self.cfg.get_value(section='directories', key='output_dir')

        # Make output directory if it does not yet exist.
        os.system(f"mkdir -p {self.output_dir}")

        # JsonWriter object
        outfile = self.cfg.get_value(section='json_out', key='output_filename')
        outpath = os.path.join(self.output_dir, outfile)
        self.ulog.print_and_logger_info(f'Output file: {outpath}')
        pretty = self.cfg.get_value(section='json_out', key='pretty')
        indent = self.cfg.get_value(section='json_out', key='indent')
        self.json_writer = JsonWriter(outpath=outpath, pretty=pretty, indent=indent)

        # UMLS reader object
        # During its instantiation, the UmlsReader object will read
        # UMLS source files to build common DataFrames used
        # to construct both nodes and rels lists. JkgWriter will use
        # the UmlsReader to read other UMLS files
        # when they are needed.
        self.ureader = UmlsReader(cfg=cfg, ulog=ulog)

    def write_nodes_list(self):
        """
        Builds and writes the nodes list of the JKG file.

        """

        list_nodes= (self._get_source_node_list() +
                     self._get_semantic_node_label_list() +
                     self._get_rel_label_list() +
                     self._get_concept_nodes_list())

        self.json_writer.write_list(list_content=list_nodes, keyname='nodes', mode='w')

    def _get_source_node_list(self) -> list:
        """
        Builds the list of Source nodes for the nodes array of the JKG.JSON.
        """

        # Obtain sorted current English-language SABs from MRSAB.RRF.
        colsabs = ['VSAB', 'RSAB', 'SON', 'SRL', 'TTYL']
        # VSAB - versioned source
        # RSAB - root source
        # SON - official name of source
        # SRL - source restriction level - can be used to filter out a licensed SAB
        # TTYL - term types from the SAB
        df = self.ureader.get_umls_file(filename='MRSAB', cols=colsabs)
        df = df.sort('RSAB')

        # Convert to a list of dictionaries for row-wise processing
        rows = df.to_dicts()

        # Build JSON output row by row.
        # Start with hard-coded rows for:
        # - the UMLS itself
        # - NDC

        listsources = [{"labels": ["Source"],
                        "properties": {"id": "UMLS:UMLS", "name": "Unified Medical Language System",
                                       "description": "United States National Institutes of Health (NIH) National Library of Medicine (NLM) Unified Medical Language System (UMLS) Knowledge Sources.",
                                       "sab": "UMLS",
                                       "source": "http://www.nlm.nih.gov/research/umls/licensedcontent/umlsknowledgesources.html"}},
                       {"labels": ["Source"],
                        "properties": {"id": "UMLS:NDC", "name": "National Drug Codes", "sab": "NDC"}}]

        for row in tqdm(rows, desc="Building Source nodes"):
            dict_node = {
                "labels": ["Source"],
                "properties": {
                    "id": f"UMLS:{row['VSAB']}",
                    "name": row["SON"],
                    "sab": row["RSAB"],
                    "srl": row["SRL"],
                    "ttyl": row["TTYL"].split(",") if row["TTYL"] else []  # Convert TTYL to a list or an empty list
                }
            }
            listsources.append(dict_node)

        return listsources

    def _get_semantic_node_label_list(self) -> list:
        """
        Builds the list of Node_Label nodes corresponding to the
        Semantic Network for the nodes array of the JKG.JSON.

        """

        list_nodes = []

        # Obtain the common semantic definitions dataset built by the
        # UmlsReader object at its initialization.
        df = self.ureader.df_semantic_definitions
        df = df.filter(pl.col('RT') == 'STY')

        # Convert to a list of dictionaries for row-wise processing
        rows = df.to_dicts()

        # Build JSON output row by row.
        for row in tqdm(rows, desc="Building Semantic Network Node_Label nodes"):
            dict_node = {
                "labels": ["Node_Label"],
                "properties": {
                    "id": f"UMLS:{row['UI']}",
                    "def": row["DEF"],
                    "node_label": row["STY_RL"],
                    "sab": "UMLS"}
            }
            list_nodes.append(dict_node)

        return list_nodes

    def _get_rel_label_list(self) -> list:
        """
        Builds the list of Rel_Label nodes for the nodes array of the JKG.JSON.

        """
        list_nodes = []

        #Get the DataFrame of concept-concept relationships built by the UmlsReader object.
        df = self.ureader.df_concept_concept_rels
        # Select the relationship labels.
        df = df.select('rel_label').unique().sort('rel_label')

        # Convert the columnar Polars DataFrame to dicts for row-level processing.
        rows = df.to_dicts()

        for row in tqdm(rows, desc="Building Rel_Label nodes from concept-concept relationships"):
            dict_node = {
                "labels": ["Rel_Label"],
                "properties": {
                    "id": f"UMLS:{row["rel_label"]}",
                    "def": row["rel_label"],
                    "rel_label": row["rel_label"],
                    "sab": "UMLS"}
            }
            list_nodes.append(dict_node)

        return list_nodes

    def _get_concept_labels_list(self) -> pl.DataFrame:
        """
        Obtains a DataFrame that aggregates the semantic types for each CUI.

        """

        # Get semantic atoms (terms) by concept.
        colsem = ['CUI', 'STY']
        # CUI
        # STY - semantic type
        df = self.ureader.get_umls_file(filename='MRSTY', cols=colsem)

        # normalize empty STY to null so they don't become empty strings in the lists
        df = df.with_columns(
            pl.when(pl.col("STY") == "").then(None).otherwise(pl.col("STY")).alias("STY")
        )

        # Group semantic type labels by CUI; aggregate into lists; then append "Concept" to each list.
        dfsty = (
            df.group_by("CUI", maintain_order=True)
            .agg(pl.col("STY"))
            .with_columns((pl.concat_list(pl.lit("Concept"), "STY")).alias("labels"))
        )

        return dfsty

    def _get_concept_nodes_list(self) -> list:
        """
        Builds the list of Concept nodes of the nodes array of the JKG.JSON.
        """
        list_nodes = []

        #Obtain the subset of English-language, non-suppressed records from
        # MRCONSO built by the UmlsReader object.
        df = self.ureader.df_concept_code_rels

        # Identify the preferred term for each concept from
        #   -- the Atom Status (TS) is preferred  (ISPREF=Y)
        #   -- the String type (STT) is "PF" (Preferred form of term)
        #   -- the Term Status is "P" (Preferred LUI of the CUI)
        df = (df.filter(pl.col('ISPREF') == 'Y')
              .filter(pl.col('STT') == 'PF')
              .filter(pl.col('TS') == 'P')
              .unique())

        # Obtain sorted list of concept labels for each concept.
        dflabels = self._get_concept_labels_list()

        df = df.join(dflabels,
                     how='inner',
                     on='CUI')

        rows = df.to_dicts()

        for row in tqdm(rows, desc="Building Concept nodes"):
            dict_node = {
                "labels": row["labels"],
                "properties": {
                    "id": f"UMLS:{row["CUI"]}",
                    "pref_term": row["STR"],
                    "sab": "UMLS"}
            }
            list_nodes.append(dict_node)

        return list_nodes

    def write_rels_list(self):
        """
        Builds the list of relations array of the JKG.JSON.

        """
        # Build rels array:
        # 1. Semantic relationships
        # 2. Concept-concept relationships
        # 3. Concept-code relationships
        # 4. Add maps of NDC codes to CUIs to rels

        #list_rels = self._get_semantic_rel_list()
        list_rels = self._get_concept_concept_rel_list()

        self.json_writer.write_list(list_content=list_rels, keyname='rels', mode='w')

        return list_rels

    def _get_semantic_rel_list(self) -> list:
        """
        Builds the list of semantic rels array of the JKG.JSON.
        """
        list_rels = []

        # Obtain the common semantic definitions dataset built by the
        # UmlsReader object at its initialization.
        df = self.ureader.df_semantic_definitions

        # Obtain from SRSTRE1 (Fully inherited set of Relations (UI's),
        # a file of the Semantic Network.
        # Filter to the basic hierarchical relationships (T186).
        df_srs = (self.ureader.get_umls_file(filename='SRSTRE1')
                  .filter(pl.col('UI2')=='T186'))

        # Join semantic definition DataFrame with the relations DataFrame
        # to obtain isa relationships between elements of the Semantic
        # Network.
        df = df.join(df_srs,
                     how='inner',
                     left_on='UI',
                     right_on='UI1',
                     maintain_order='left').sort(['UI','UI3'])

        rows = df.to_dicts()
        for row in tqdm(rows, desc="Building semantic rels array"):
            dict_rel = {
                "label": "isa",
                "end": {
                    "properties" : {
                        "id": f"UMLS:{row["UI3"]}"
                    }
                },
                "properties":{
                            "sab":"UMLS"
                },
                "start": {
                    "properties" : {
                        "id": f"UMLS:{row["UI"]}"
                    }
                }
            }

            list_rels.append(dict_rel)

        return list_rels

    def _get_concept_concept_rel_list(self) -> list:
        """
        Builds the list of concept-concept rels array of the JKG.JSON.
        """
        list_rels = []

        # Obtain the common concept-concept relationship dataset built by the
        # UmlsReader object at its initialization.
        df = self.ureader.df_concept_concept_rels

        rows = df.to_dicts()
        for row in tqdm(rows, desc="Building concept-concept rels array"):

            # In the concept-concept relationship DataFrame,
            # CUI2 identifies the start concept and CUI1 identifies
            # the end concept of the relationship.
            dict_rel = {
                "label": f"{row["rel_label"]}",
                "end": {
                    "properties" : {
                        "id": f"UMLS:{row["CUI1"]}"
                    }
                },
                "properties":{
                            "sab": row["SAB"]
                },
                "start": {
                    "properties" : {
                        "id": f"UMLS:{row["CUI2"]}"
                    }
                }
            }

            list_rels.append(dict_rel)

        return list_rels


