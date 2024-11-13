import os
from modules.sql_parser.parse_nodes import extract_nodes
from modules.sql_parser.parse_lineages import extract_lineages
from modules.sql_parser.extraction_sqlglot import preprocess_queries
from sankeyapp.app import main 


def remove_files_from_paths(*paths: str) ->None:
    """
    Clean all 
    """
    for path in paths:
        for file in os.listdir(path):
            if os.path.isfile(f'{path}{file}'):
                os.remove(f'{path}{file}')


def sql_to_lineages():
    """
    Orchestrator
    """
    remove_files_from_paths('data/output-tables/', 'data/output-tables/lineages/')
    preprocessed_queries = preprocess_queries('data/queries-txts/TEST.txt') # 'data/queries-txts/WorldWideImporters 1.txt'
    nodes = extract_nodes(preprocessed_queries)
    lineages = extract_lineages(preprocessed_queries, nodes)
    main('C:/Users/PietroGarroni/projects/sql_code_parser/data/output-tables/lineages/', 'C:/Users/PietroGarroni/projects/sql_code_parser/data/output-tables/nodes.csv')



if __name__ == '__main__':
    sql_to_lineages()   








