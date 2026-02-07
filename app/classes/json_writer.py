#!/usr/bin/env python
# coding: utf-8
"""
Class that writes to a JSON file, with a TQDM progress bar.

Format options:

- pretty-printing with spacing, indentation, and line feeds for legibility
- minimal (but not no) spacing, line feeds, and indentation

The difference in file size resulting from large numbers of data elements can
be significant for the two format options.
"""

import json
import textwrap
from tqdm import tqdm

class JsonWriter:

    def __init__(self, outpath: str, pretty: bool=False, indent: int=4):
        # Path to JSON output file
        self.outpath = outpath
        # Whether to "pretty print" or write with minimal spacing and indentation
        self.pretty = pretty
        # Indentation if pretty printing
        self.indent = indent

    def write_list(self, list_content: list, keyname: str, mode: str):
        """
        Writes a list to the JSON file as the value of a key, using
        a TQDM progress bar.
        :param keyname: name of the key for which the list is the value
        :param mode: w (write) or a (append)
        :param list_content: list to write

        The use case is: write {"keyname":[list of elements]}

        Caveat emptor: This routine assumes that list_content contains JSON-compliant strings.
        """

        write_pretty = self.pretty == "true"
        indent_spaces = int(self.indent)
        node_indent = " " * indent_spaces
        top_indent = " " * (indent_spaces // 2)

        with open(self.outpath, mode, encoding="utf-8") as f:
            f.write('{\n' + top_indent + '"' + keyname + '":' + top_indent + '[\n')

            for i, node in enumerate(tqdm(list_content, desc=f"Writing {keyname} array", total=len(list_content))):

                if i:
                    # Line feed at the start of each node.
                    f.write(",\n")

                # Produce node JSON either as pretty (multi-line) or minimal (one-line, with minimal indentation)
                if isinstance(node, str):
                    node_json = node.strip()
                else:
                    if write_pretty:
                        # Pretty-print the node.
                        node_json = json.dumps(node, ensure_ascii=False, indent=indent_spaces)
                        node_indented = textwrap.indent(node_json, node_indent)

                    else:
                        # Use minimal separator spacing, with a small indent.
                        node_json = json.dumps(node, ensure_ascii=False, separators=(',', ':'))
                        node_indented = node_indent + node_json

                    f.write(node_indented)

            # Close array and object using the same top indentation.
            f.write("\n" + top_indent + "]\n}\n")