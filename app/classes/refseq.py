"""
refseq.py
Class that obtains definitions of genes from RefSeq,
using the NCBI EUtils REST API.
"""
import os
import time
import requests
from .ubkg_logging import UbkgLogging
from .ubkg_extract import ubkgExtract

class Refseqapi:

    def _getapikey(self) -> str:

        # Get an API key from a text file in the application directory.
        try:
            apikey_path = os.path.join(os.getcwd(), 'classes', 'apikey.txt')
            with open(apikey_path, 'r') as fapikey:
                apikey = fapikey.read()
        except FileNotFoundError as e:
            self.ulog.print_and_logger_info('Missing file: apikey.txt')
            exit(1)

        return apikey
    def __init__(self, ulog:UbkgLogging):

        self.ulog = ulog
        self.apikey = self._getapikey()
        self.uextract = ubkgExtract(self.ulog)

    def get_gene_definition(self, hgnc_id: str) -> str:
        """
        Retrieve gene summary/definition from NCBI eutils given an HGNC ID.

        :param hgnc_id: HGNC ID e.g. 'HGNC:1097'
        :return: gene summary string
        """

        base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"

        # NCBI rate limits: 3 requests/sec without API key, 10 requests/sec with API key
        delay = 0.11 if self.apikey else 0.34
        time.sleep(delay)

        # Step 1: Use esearch to find the NCBI Gene ID from the HGNC ID
        esearch_url = f"{base_url}/esearch.fcgi"
        esearch_params = {
            "db": "gene",
            "term": f"{hgnc_id}[xref]",
            "retmode": "json",
            "api_key": self.apikey
        }
        esearch_response = requests.get(esearch_url, params=esearch_params)

        if esearch_response.status_code == 429:
            # Back off and retry once
            time.sleep(2)
            esearch_response = requests.get(esearch_url, params=esearch_params)

        esearch_response.raise_for_status()
        esearch_data = esearch_response.json()

        id_list = esearch_data.get("esearchresult", {}).get("idlist", [])
        if not id_list:
            return ""

        ncbi_gene_id = id_list[0]

        time.sleep(delay)

        # Step 2: Use esummary to get the gene summary
        esummary_url = f"{base_url}/esummary.fcgi"
        esummary_params = {
            "db": "gene",
            "id": ncbi_gene_id,
            "retmode": "json",
            "api_key": self.apikey
        }
        esummary_response = requests.get(esummary_url, params=esummary_params)

        if esummary_response.status_code == 429:
            # Back off and retry once
            time.sleep(2)
            esummary_response = requests.get(esummary_url, params=esummary_params)

        esummary_response.raise_for_status()
        esummary_data = esummary_response.json()

        summary = (esummary_data
                   .get("result", {})
                   .get(ncbi_gene_id, {})
                   .get("summary", ""))

        return summary if summary else ""





