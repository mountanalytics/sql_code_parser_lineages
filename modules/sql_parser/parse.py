
from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import sqlglot
import pandas as pd
import configparser
import os
import json
import re
from .parse_nodes import *
from .parse_lineages import *

def extract(preprocessed_queries:list) -> pd.DataFrame:
    """
    Orchestrates the extraction of the nodes from a list of queries, the output being a nodes pd.DataFrame
    """

    # create nodes
    nodes_dfs = []
    lineages_dfs =[]
    queries= []

    node_num = 0

    for i, query in enumerate(preprocessed_queries):
        node_num +=1
        filename = f"file_{node_num}"
        query_node = f"query_{node_num}"

        lineages = None

        if query['type'] == 'update_or_create_select': # or  query['type'] == 'select' ### to add select without create or update


            nodes = parse_update_or_create_select_nodes(query, i)
            lineages, destination = parse_update_or_create_select_lineages(query, i, query_node)


        elif query['type'] == 'update_or_create_set':
            nodes = parse_update_or_create_set_nodes(query, query_node)
            lineages, destination = parse_update_or_create_set_lineages(query, query_node)


        elif query['type'] == 'declare':
            nodes = parse_declare_nodes(query)

        else:
            nodes = None
            continue


        if nodes != None:
            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)

        if lineages != None:

            lineages_df = create_lineages_df(lineages, nodes, filename, destination)
            lineages_dfs.append(lineages_df)


    nodes = create_nodes_df(nodes_dfs)

    return nodes, lineages_dfs


if __name__ == '__main__': #python -m modules.sql_parser.parse_nodes
    from modules.sql_parser.extraction_sqlglot import preprocess_queries

    preprocessed_queries = preprocess_queries('data/queries-txts/queries_rabo_qrm.txt') # 'data/queries-txts/WorldWideImporters 1.txt'

    nodes = extract_nodes(preprocessed_queries)