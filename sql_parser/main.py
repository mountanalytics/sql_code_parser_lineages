import os
from .modules.parse_nodes import extract_nodes, get_rationalization_score
from .modules.parse_lineages import extract_lineages
from .modules.extraction_sqlglot import preprocess_queries_ssis
from sankeyapp.app import main 

def main(query, result_set, nodes, lineages, variable_tables, node_name):
    """
    Orchestrator
    """
    preprocessed_queries = preprocess_queries_ssis(query, result_set, node_name) 
    nodes_q, variable_tables = extract_nodes(preprocessed_queries, node_name, variable_tables)

    nodes_flag = get_rationalization_score(preprocessed_queries, node_name)
    lineages_q = extract_lineages(preprocessed_queries, nodes_q, node_name)

    return nodes_q, lineages_q, variable_tables, nodes_flag