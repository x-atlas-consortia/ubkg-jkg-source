"""
Functions to create standardized identifiers for codes and terms from UMLS
vocabularies.

The desired format for UBKG identifiers is CURIE (Compact URI), or
<SAB>:<CODE>
where SAB corresponds to a Source ABbreviation for the source vocabulary.

Although the majority of UMLS vocabularies conform to this format,
a small number of important vocabularies do not. Examples of
non-conformance include GO, which uses the format GO:GO:12345.

In addition, some vocabularies include special characters in codes,
including those in [',','/',' ','<','>','+','*','&','#'].

Functions in this script:
1. strip special characters
2. standardize to CURIE format
3. adjust terms that resemble CURIEs

"""
import polars as pl
import re

def create_codeid(SAB_col: pl.Expr, CODE_col: pl.Expr, codeid_col: str) -> pl.Expr:
    """
    Creates a CURIE-like code id by concatenating SAB and CODE columns.
    :param SAB_col: a Polars expression for the 'SAB' column--i.e., the column from a DataFrame corresponding
                    to a Source ABbreviation (SAB) for a vocabulary
    :param CODE_col: a Polars expression for the 'CODE' column--i.e., the column from a DataFrame corresponding
                    to a code from a vocabulary
    :param codeid_col: the name of the column in the DataFrame that will contain the standarized ID

    :return: CURIE-like expression for the codeid
    """

    return (SAB_col + ":" + CODE_col).alias(codeid_col)

def standardize_codeid(codeid_col: str) -> pl.Expr:
    """
    Creates a standardized codeid in CURIE format
    for codes from UMLS vocabularies.

    :param codeid_col: the name of the column in the DataFrame
                       that will contain the standarized ID

    Assumes that the codeid column has already been created from
    the SAB and CODE columns of a DataFrame.

    :return: a Polars expression for the standardized codeid

    """

    return (
        # Transform IDs with embedded SAB (e.g., GO:GO:12345)
        (pl.col(codeid_col)
         .str.split(':')  # Split the `codeid` on ':'
         .list.get(0)  # Get the first part
         + ':' +
         pl.col(codeid_col)
         .str.split(':')
         .list.get(-1))  # Get the last part
        .str.replace(',', '_', literal=True)  # Replace ',' with '_'
        .str.replace('/', '_', literal=True)  # Replace '/' with '_'
        .str.replace(' ', '_', literal=True)  # Replace ' ' with '_'
        .str.replace('<', '__', literal=True)  # Replace '<' with '__'
        .str.replace('>', '_', literal=True)  # Replace '>' with '_'
        .str.replace('+', '-', literal=True)  # Replace '+' with '-'
        .str.replace('*', '-', literal=True)  # Replace '*' with '-'
        .str.replace('&', '.', literal=True)  # Replace '&' with '.'
        .str.replace('#', '.', literal=True)  # Replace '#' with '.'
        .alias(codeid_col) # Alias back to the same `codeid`
    )

def standardize_term(term_col: str) -> pl.Expr:
    """
    Adds a colon to a term if it resembles a CURIE.
    :param term_col: the name of a column in the DataFrame that will contain the standarized term
    :return: a Polars expression for the standardized term

    """

    # Define the regex pattern for CURIE-like terms
    pattern = r'^[a-zA-Z0-9._-]+:[a-zA-Z0-9._-]+$'

    return (
        pl.when(pl.col(term_col).str.contains(pattern))
        .then(pl.col(term_col) + ":")  # Add ':' if it matches the regex
        .otherwise(pl.col(term_col))  # Otherwise, keep the value unchanged
        .alias(term_col)  # Alias back to the same column name
    )
