from sqlglot import parse_one, exp
from sqlglot.dialects.ma import MA
from sqlglot.dialects.tsql import TSQL
import pypyodbc as odbc
import configparser
import copy
from collections import defaultdict
from collections import OrderedDict 
import pandas as pd
import pypyodbc as odbc
import configparser
import os
import json
import re
from modules.sql_parser.parse_nodes import *
from modules.sql_parser.parse_lineages import *
from sankeyapp.app import main 


def remove_files_from_paths(*paths):
    for path in paths:
        for file in os.listdir(path):
            if os.path.isfile(f'{path}{file}'):
                os.remove(f'{path}{file}')


def open_query(dir:str):
    # open the sql queries from one single file
    with open(dir, 'r') as file: 
        file = file.read().strip().split(';')
        sql_queries = [re.sub(r'\s+', ' ', query.strip().replace('\n', ' ').replace('\t', ' ')) for query in file if query.strip()]
    return sql_queries


def extract_subqueries(ast):
    count = 0
    selects = list(ast.find_all(exp.Select))   
    selects = [str(select) for select in selects]
    nested_subqueries = {}

    for i, subquery_i in enumerate(selects):
            for j, subquery_j in enumerate(selects):
                    if subquery_j in subquery_i and j != i:                      
                            count +=1
                            subquery_i = subquery_i.replace(subquery_j, f"subquery_{count}")
                            nested_subqueries[f"subquery_{i}"] = subquery_i
                            nested_subqueries[f"subquery_{j}"] = subquery_j

    subqueries = {k: v for k, v in nested_subqueries.items() if k != 'subquery_0'}
    #print(subqueries)

    return subqueries


def replace_subqueries_in_mainquery(ast, subqueries_global):
    subqueries = subqueries_global.copy()
    try:
        main_query = str(list(ast.find_all(exp.Create))[0])  
    except:
        main_query = str(list(ast.find_all(exp.Insert))[0])

    

    # replace nested subqueries in subqueries
    for i, query_i in subqueries.items():
        for j, query_j in subqueries.items():
            if j in query_i:
                subqueries[i] = subqueries[i].replace(j, query_j)

    for key_i, value_i in subqueries.items():
            
        main_query = main_query.replace(f"({str(value_i)})", str(key_i))
        main_query = main_query.replace(value_i, str(key_i))

    return main_query


def save_preprocessed_query(preprocessed_query, idx):
    filename = f'data/preprocessed-queries/json_data{idx}.json'
    with open(filename, 'w') as json_file:
        json.dump(preprocessed_query, json_file, indent=4)


def preprocess_queries(dir):
    preprocessed_queries = []
    sql_queries = open_query(dir)
    for i, query in enumerate(sql_queries):
        ast = parse_one(query, read="ma")
        subqueries = extract_subqueries(ast)
        main_query = replace_subqueries_in_mainquery(ast, subqueries)
        print(subqueries)
        preprocessed_query = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries}
        preprocessed_queries.append(preprocessed_query)

    return preprocessed_queries


def reverse_subqueries(preprocessed_queries):

    reversed_preprocessed_queries = []

    for query in preprocessed_queries:
        query_subqueries = dict(reversed(list(query['subquery_dictionary'].items())))
        query_subqueries['main_query'] = query['modified_SQL_query']
        reversed_preprocessed_queries.append(query_subqueries)
        
    return reversed_preprocessed_queries


def clean_query(query):
    if query.startswith("("): # remove open and closing paranthesis from subqueries
        query = query.strip("()")
    else:
        pass
    ast = parse_query(query) # get parsed tree
    tree = replace_aliases(query) # get transformed tree without table aliases
    select_statement_big = tree.find_all(exp.Select) # parse selects before getting statements
    return ast, select_statement_big


def add_node_subquery(nodes, query_name, file_name, where_exp, on_condition):
    target_node = query_name
    #nodes.append({'NAME_NODE': f"{target_node}", 'LABEL_NODE': f'{file_name}@{target_node}', 'FILTER': where_exp, 'FUNCTION': 'subquery', 'ON': on_condition})
    nodes.append({'NAME_NODE': target_node, 'LABEL_NODE': f'{file_name}@{target_node}', 'FILTER': where_exp, 'FUNCTION': 'subquery', 'ON': on_condition})
    return nodes


def add_node_mainquery(nodes, ast, where_exp, on_condition):
    try: # try to find the create or insert into statements
        target_node = list(ast.find_all(exp.Create))[0].this.this.this
        nodes.append({'NAME_NODE': f"query_{target_node}",'LABEL_NODE': f"query_{target_node}", 'FILTER': where_exp, 'FUNCTION': 'query', 'ON': on_condition})
        nodes.append({'NAME_NODE': target_node,'LABEL_NODE': target_node, 'FILTER': None, 'FUNCTION': 'target', 'ON': None})
    except IndexError:
        target_node = list(ast.find_all(exp.Insert))[0].this.this
        nodes.append({'NAME_NODE': f"query_{target_node}",'LABEL_NODE': f"query_{target_node}", 'FILTER': where_exp, 'FUNCTION': 'query', 'ON': on_condition})
        nodes.append({'NAME_NODE': target_node,'LABEL_NODE': target_node, 'FILTER': where_exp, 'FUNCTION': 'target', 'ON': on_condition})
    return nodes


def add_node_sourcetables(nodes, source_tables, file_name, on_condition):
    for table in source_tables:
        if table not in [node['NAME_NODE'] for node in nodes]:
            if 'subquery' in table: 
                nodes.append({'NAME_NODE': table,'LABEL_NODE': f'{file_name}@{table}', 'FILTER': None, 'FUNCTION': 'subquery', 'ON': on_condition})
            else:
                nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataSources', 'ON': None})
    return nodes


def append_convert_nodes_to_df(nodes_dataframes, nodes):
    nodes = pd.DataFrame(nodes)
    nodes['COLOR'] = nodes['FILTER'].apply(lambda x: '#db59a5' if x is not None else '#42d6a4')
    nodes_dataframes.append(pd.DataFrame(nodes))   
    return nodes_dataframes


def create_nodes_df(nodes_dfs):
    nodes = pd.concat(nodes_dfs).reset_index(drop=True)
    nodes['ON'] = nodes['ON'].apply(lambda x: None if x == [] else x) # remove empty lists
    nodes = nodes.drop_duplicates(subset=['NAME_NODE', 'LABEL_NODE', 'FILTER', 'FUNCTION']).reset_index(drop=True)
    nodes['ID'] = nodes.index
    nodes.to_csv('data/output-tables/nodes.csv') # save df

    return nodes


def extract_nodes(reversed_preprocessed_queries):
    # create nodes
    nodes_dfs = []
    queries= []
    for i, query_subqueries in enumerate(reversed_preprocessed_queries):
        filename = f"file_{i}"
        nodes = []
        for name_query in query_subqueries:

            # CLEAN QUERY
            ast, select_statement_big = clean_query(query_subqueries[name_query])

            source_tables = []
            # for every select statement in query, extract the source tables, where expressions and on conditions
            for select in list(select_statement_big):
                source_table, where_exp, on_condition = get_statements(select) 
                source_tables += source_table            

            if 'subquery' in name_query: # if the query is a subquery then the name is the dict key, else the name is the target table
                nodes = add_node_subquery(nodes, name_query, filename, where_exp, on_condition)
            else:
                nodes = add_node_mainquery(nodes, ast, where_exp, on_condition)
            nodes = add_node_sourcetables(nodes, source_tables, filename, on_condition)
        nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)

    nodes = create_nodes_df(nodes_dfs)

    return nodes


        
def create_lineages_df2(reversed_preprocessed_queries):
    # extract lineages

    lineages_dfs = []
    trees = []
    queries=[]
    for i, query_subqueries in enumerate(reversed_preprocessed_queries):
        filename = f"file_{i}"

        lineages = [] # list of dictionaries with the nodes

        for name_query in query_subqueries:

            # remove parenthesis around query
            query = query_subqueries[name_query]
            if query.startswith("("):
                query = query.strip("()")
            else:
                pass

            ast = parse_query(query) # get parsed tree

            if 'subquery' in name_query: # if the query is a subquery then the name is the dict key, else the name is the target table
                target_node = name_query
                target_columns =[]
            else:
                target_columns =[]
                try: # try with create table statement
                    target_node = list(ast.find_all(exp.Create))[0].this.this.this
                except IndexError: # else try with insert into table statement
                    target_node = list(ast.find_all(exp.Insert))[0].this.this

                    insert_obj = list(ast.find_all(exp.Insert))[0]
                    target_columns = list(insert_obj.find_all(exp.Column))
                    target_columns = [[i] for i in target_columns]   

            space_table = find_table_w_spaces(ast) # list with tables with spaces (sqlglot cant parse them)
            space_table = list(set(space_table)) # a list of tuples with table names paired (space removed original - original ) Eg. (OrderDetails, Order Details)
            alias_table = get_tables(ast) # parse table name + table alias
            tree = replace_aliases(query) # transform query by removing table aliases

            if target_columns == []:
                select_statement, target_columns = extract_target_columns(tree) # extract target columns
                #print(target_columns)
            else:
                select_statement, x = extract_target_columns(tree) # extract target columns

            replaced_trees = [x.transform(transformer_functions) for x in select_statement] # replace columns aliases
            trees.append(replaced_trees)

            # add possible transformation to columns
            transformations = extract_transformation(replaced_trees)
            target_columns = list(zip(target_columns, transformations)) 
            query_node = f'query_{target_node}'

            lineages = extract_source_target_transformation(target_columns, lineages, space_table, query_node, target_node) # append lineages of node to list

        lineages = pd.DataFrame(lineages)

        lineages = lineages.explode('SOURCE_COLUMNS').reset_index()

        lineages['FILE_NAME'] = filename
        lineages['ROW_ID'] = 0
        lineages['LINK_VALUE'] = 1

        lineages['SOURCE_NODE'] = lineages['SOURCE_COLUMNS'].apply(lambda x:".".join(x.split('.')[0:-1]))
        lineages['TARGET_NODE'] = lineages['TARGET_COLUMN'].apply(lambda x:".".join(x.split('.')[0:-1]))

        lineages['SOURCE_FIELD'] = lineages['SOURCE_COLUMNS'].apply(lambda x:x.split('.')[-1])
        lineages['TARGET_FIELD'] = lineages['TARGET_COLUMN'].apply(lambda x:x.split('.')[-1])

        lineages['SOURCE_NODE'] = [f'{filename}@{i}' if 'subquery' in i else i for i in lineages['SOURCE_NODE'] ]
        lineages['TARGET_NODE'] = [f'{filename}@{i}' if 'subquery' in i else i for i in lineages['TARGET_NODE']]

        lineages['COLOR'] =  ["aliceblue" if i == "" else "orangered" for i in lineages['TRANSFORMATION']]

        # merge source id
        lineages = pd.merge(lineages, nodes[['ID', 'LABEL_NODE']], left_on='SOURCE_NODE', right_on = 'LABEL_NODE', how='left')
        lineages['SOURCE_NODE'] = lineages['ID']
        lineages.drop(columns=['ID', 'LABEL_NODE'], inplace=True)

        # merge target id
        lineages = pd.merge(lineages, nodes[['ID', 'LABEL_NODE']], left_on='TARGET_NODE', right_on = 'LABEL_NODE', how='left')
        lineages['TARGET_NODE'] = lineages['ID']
        lineages.drop(columns=['ID', 'LABEL_NODE'], inplace=True)

        lineages = lineages.drop_duplicates(subset = ['SOURCE_COLUMNS', 'TARGET_COLUMN', 'TRANSFORMATION']).reset_index(drop=True)

        lineages.to_csv(f"data/output-tables/lineages/lineage-{target_node}.csv")
        lineages_dfs.append(lineages)
        
    return lineages_dfs


def create_lineages_df(reversed_preprocessed_queries):


    for idx, query in enumerate(reversed_preprocessed_queries):

        lineages = []
        filename = f"file_{idx}"



        for component in query.keys():

            if "subquery" in component:

                for key, value in query.items():
                    if component in value:
                        target_node = key
                        print(component)
                        print(target_node)
            print()

        destination =  list(parse_query(query['main_query']).find_all(exp.Create))[0].this.this.this


        for component in query.keys():

            target_columns =[]

            ast = parse_query(query[component])
            space_table = find_table_w_spaces(ast) # list with tables with spaces (sqlglot cant parse them)
            alias_table = get_tables(ast) # parse table name + table alias
            tree = replace_aliases(query[component]) # transform query by removing table aliases

            select_statement, target_columns = extract_target_columns(tree) # extract target columns

            transformations = extract_transformation(select_statement)
            target_columns = list(zip(target_columns, transformations)) 


            # next node in case of subquery in subquery, subquery in main_query or main_query
            if "subquery" in component:
                for key, value in query.items():
                    if component in value: # if the component is in the query 
                        if 'subquery' in key: # subquery in subquery
                            query_node=component
                            target_node_next = key
                        else: # subquery in main_query
                            query_node=component
                            target_node_next = f'query_{destination}'

            else: # main_query
                query_node = f'query_{destination}'
                target_node_next =  destination


            for column in target_columns:
                for source_col in column[0]:
                    print(component)

                    print("column: ", f'{source_col.table}.{source_col.this}',  column[1])
                    print("query: ", query_node)
                    print("next: ", target_node_next)
                    print()
                    
                    # if there is a database name in the column
                    if source_col.db!="":
                        db = source_col.db + "."
                    else:
                        db = ""

                    if 'subquery' in source_col.table:
                        if column[1] == '': # add lineage with no transformation
                            #lineages.append({'SOURCE': f"{db}{source_col.table}.{source_col.this}", 'TARGET': f"{query_node}.{source_col.this}", 'TRANSFORMATION': ""})
                            lineages.append({'SOURCE': f"{query_node}.{source_col.this}", 'TARGET': f"{target_node_next}.{source_col.this}", 'TRANSFORMATION': column[1]})

                        else: # add lineage with transformation
                            #lineages.append({'SOURCE': f"{db}{source_col.table}.{source_col.this}", 'TARGET': f"{query_node}.{source_col.this}", 'TRANSFORMATION': ""})
                            lineages.append({'SOURCE': f"{query_node}.{source_col.this}", 'TARGET': f"{target_node_next}.{column[1].split('AS')[1].strip()}", 'TRANSFORMATION': column[1].split('AS')[0].strip()})

                    else:   

                        if column[1] == '': # add lineage with no transformation
                            lineages.append({'SOURCE': f"{db}{source_col.table}.{source_col.this}", 'TARGET': f"{query_node}.{source_col.this}", 'TRANSFORMATION': ""})
                            lineages.append({'SOURCE': f"{query_node}.{source_col.this}", 'TARGET': f"{target_node_next}.{source_col.this}", 'TRANSFORMATION': column[1]})

                        else: # add lineage with transformation
                            lineages.append({'SOURCE': f"{db}{source_col.table}.{source_col.this}", 'TARGET': f"{query_node}.{source_col.this}", 'TRANSFORMATION': ""})
                            lineages.append({'SOURCE': f"{query_node}.{source_col.this}", 'TARGET': f"{target_node_next}.{column[1].split('AS')[1].strip()}", 'TRANSFORMATION': column[1].split('AS')[0].strip()})


        lineages = pd.DataFrame(lineages)

        lineages['FILE_NAME'] = filename
        lineages['ROW_ID'] = 0
        lineages['LINK_VALUE'] = 1

        lineages['SOURCE_NODE'] = lineages['SOURCE'].apply(lambda x:".".join(x.split('.')[0:-1]))
        lineages['TARGET_NODE'] = lineages['TARGET'].apply(lambda x:".".join(x.split('.')[0:-1]))
        lineages['SOURCE_FIELD'] = lineages['SOURCE'].apply(lambda x:x.split('.')[-1])
        lineages['TARGET_FIELD'] = lineages['TARGET'].apply(lambda x:x.split('.')[-1])

        lineages['SOURCE_NODE'] = [f'{filename}@{i}' if 'subquery' in i else i for i in lineages['SOURCE_NODE'] ]
        lineages['TARGET_NODE'] = [f'{filename}@{i}' if 'subquery' in i else i for i in lineages['TARGET_NODE']]

        lineages['COLOR'] =  ["aliceblue" if i == "" else "orangered" for i in lineages['TRANSFORMATION']]

        # merge source id
        lineages = pd.merge(lineages, nodes[['ID', 'LABEL_NODE']], left_on='SOURCE_NODE', right_on = 'LABEL_NODE', how='left')
        lineages['SOURCE_NODE'] = lineages['ID']
        lineages.drop(columns=['ID', 'LABEL_NODE'], inplace=True)

        # merge target id
        lineages = pd.merge(lineages, nodes[['ID', 'LABEL_NODE']], left_on='TARGET_NODE', right_on = 'LABEL_NODE', how='left')
        lineages['TARGET_NODE'] = lineages['ID']
        lineages.drop(columns=['ID', 'LABEL_NODE'], inplace=True)

        lineages = lineages.drop_duplicates(subset =['SOURCE', 'TARGET', 'TRANSFORMATION']).reset_index(drop=True)

        lineages.to_csv(f"data/output-tables/lineages/lineage-{target_node_next}.csv")
     
        print()




            





if __name__ == '__main__':

    remove_files_from_paths('data/preprocessed-queries/', 'data/output-tables/', 'data/output-tables/lineages/')

    preprocessed_queries = preprocess_queries('data/queries-txts/TEST.txt') # 'data/queries-txts/WorldWideImporters 1.txt'

    reversed_preprocessed_queries = reverse_subqueries(preprocessed_queries) # reverse the subqueries order to start from the deepest level

    nodes = extract_nodes(reversed_preprocessed_queries)

    lineages = create_lineages_df(reversed_preprocessed_queries)

    main('C:/Users/PietroGarroni/projects/sql_code_parser/data/output-tables/lineages/', 'C:/Users/PietroGarroni/projects/sql_code_parser/data/output-tables/nodes.csv')








