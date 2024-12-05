from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import sqlglot
import pandas as pd
import configparser
import os
import json
import re



def parse_tables(table, table_alias_list, subquery=True):    
    """
    Parses all table information available (db, catalog...)
    """ 

    if subquery == False:
        table_alias =  table.alias.strip()
        table_name = table.name.strip()
        table_db = table.db.strip()
        table_catalog = table.catalog.strip()

    else:
        table_alias = table.alias.strip()
        source = table.this.args["from"].strip()
        table_name= source.this.name.strip()
        table_catalog =  source.this.catalog.strip()
        table_db = source.this.db.strip()
        
    if " " in table_name:
        table_name = table_name.replace(" ", "")
    if table_catalog != "" and table_db != "":
        result = (table_catalog+"."+ table_db+"."+table_name, table_alias)
    elif table_db == "" and table_catalog == "":
        result = (table_name, table_alias)
    elif table_catalog == "": 
        result = (table_db+"."+table_name, table_alias)
    elif table_db == "":
        result = (table_catalog+"."+table_name, table_alias)
        
    table_alias_list.append(result)
    return result


def get_tables(ast: sqlglot.expressions.Select):
    """
    Extracts the table names and their aliases, used to reconstruct a tuple with structure (database+schema+name, alias )
    """
    # find all tables
    table_alias = list(ast.find_all(exp.Table))
    alias_table = []
    # extract information from each table
    for table in table_alias:
        try:
            parse_tables(table, alias_table, False)
        except:
            pass

    return alias_table


def replace_aliases(ast: sqlglot.expressions) -> sqlglot.expressions:
    """
    Replaces the tables' aliases in a query
    """
    ast = list(ast.find_all(exp.Select))[0]
    alias_table = get_tables(ast)
    
    def transformer_table(node):
        for element in alias_table:
            if isinstance(node, exp.Column) and node.table == element[1]:
                return parse_one(element[0] + "." + node.name)
        return node

    transformed_tree = ast.transform(transformer_table)

    return transformed_tree

def extract_from_statements(tree: sqlglot.expressions.Select, source_tables:list) -> list:
    """
    Extracts the from statement from the input query (tree)
    """
    from_exp = list(tree.find_all(exp.From))
    from_table =str(from_exp[0].this).split(' AS')[0] # table
    source_tables.append(from_table)
    return source_tables


def extract_join_statements(tree: sqlglot.expressions, source_tables: list) -> list:
    """
    Extracts the join statement from the input query (tree)
    """
    join_exp = list(tree.find_all(exp.Join))
    if join_exp != []:
        join_table = str(join_exp[0].this).split(' AS')[0] # table
        source_tables.append(join_table)
    else:
        join_exp = None
    return source_tables


def extract_where_statements(tree: sqlglot.expressions) -> str:
    """
    Extract the where statement from the input query (tree)
    """
    where_exp = list(tree.find_all(exp.Where))
    if where_exp != []:
        where_exp = str(where_exp[0].this).split(' AS')[0]# table
    else:
        where_exp = None
    return where_exp


def extract_on_statements(tree: sqlglot.expressions) -> list:
    """
    Function to extract the on condition from the join statements, (on column = column)
    """
    # parse select statement
    select = list(tree.find_all(exp.Select))[0]

    # parse join statements
    joins = list(select.find_all(exp.Join))
    on_conditions = []
    for join in joins:
        try:
            on_conditions.append(f"{list(join.find_all(exp.EQ))[0].this.table}.{list(join.find_all(exp.EQ))[0].this.this} = {list(join.find_all(exp.EQ))[0].expression.table}.{list(join.find_all(exp.EQ))[0].expression.this}")
        except:
            return []
        
    if joins != []:
        return on_conditions
    else:
        return []


def get_statements(tree: sqlglot.expressions) -> tuple[list, str, list]:
    """
    Extracts from expression, join expression and where expression from query
    """

    source_tables = []
    # from expression
    source_tables = extract_from_statements(tree, source_tables)
    # join expression
    source_tables = extract_join_statements(tree, source_tables)
    # where expression
    where_exp = extract_where_statements(tree)
    # join expressions
    on_exp = extract_on_statements(tree)

    return source_tables, where_exp, on_exp




def clean_query(ast: sqlglot.expressions) -> sqlglot.expressions.Select:
    """
    Cleans the query and converts it in a sqlglot select statement
    """
    tree = replace_aliases(ast) # get transformed tree without table aliases
    select_statement_big = tree.find_all(exp.Select) # parse selects before getting statements
    return select_statement_big


def add_node_subquery(nodes:list, query_name:str, file_name:str, where_exp:str, on_condition:str) -> list:
    """
    Add a node to the nodes list
    """
    target_node = query_name
    nodes.append({'NAME_NODE': target_node, 'LABEL_NODE': f'{file_name}@{target_node}', 'FILTER': where_exp, 'FUNCTION': 'subquery', 'ON': on_condition, 'COLOR': '#d0d3d3'})
    return nodes


def add_node_mainquery(nodes:list, ast: sqlglot.expressions, where_exp, on_condition) -> list:
    """
    Add main query node to the nodes list
    """
    try: # try to find the create or insert into statements
        target_node = list(ast.find_all(exp.Create))[0].this.this.this
        nodes.append({'NAME_NODE': f"query_{target_node}",'LABEL_NODE': f"query_{target_node}", 'FILTER': where_exp, 'FUNCTION': 'query', 'ON': on_condition, 'COLOR': '#d0d3d3'})
        nodes.append({'NAME_NODE': target_node,'LABEL_NODE': target_node, 'FILTER': None, 'FUNCTION': 'target', 'ON': None, 'COLOR': "#42d6a4"})
    except IndexError:
        target_node = list(ast.find_all(exp.Insert))[0].this.this
        nodes.append({'NAME_NODE': f"query_{target_node}",'LABEL_NODE': f"query_{target_node}", 'FILTER': where_exp, 'FUNCTION': 'query', 'ON': on_condition, 'COLOR': '#d0d3d3'})
        nodes.append({'NAME_NODE': target_node,'LABEL_NODE': target_node, 'FILTER': where_exp, 'FUNCTION': 'target', 'ON': on_condition, 'COLOR': "#42d6a4"})
    return nodes, target_node


def add_node_sourcetables(nodes:list, source_tables:list, file_name:str, on_condition:list) -> list:
    """
    Add source tables to nodes list
    """
    for table in source_tables:
        if table not in [node['NAME_NODE'] for node in nodes]:
            if 'subquery' in table: 
                nodes.append({'NAME_NODE': table,'LABEL_NODE': f'{file_name}@{table}', 'FILTER': None, 'FUNCTION': 'subquery', 'ON': on_condition, 'COLOR': '#d0d3d3'})
            else:
                nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataSources', 'ON': None, 'COLOR': "#42d6a4"})
    return nodes


def append_convert_nodes_to_df(nodes_dataframes:list, nodes:list) -> pd.DataFrame:
    """
    Converts a node (list of dictionaries) to dataframe and appends it to list
    """
    nodes = pd.DataFrame(nodes)
    nodes['COLOR'] = nodes.apply(
        lambda row: row['COLOR'] if pd.isnull(row['FILTER']) and pd.isnull(row['ON']) else '#db59a5', 
        axis=1
    )

    nodes_dataframes.append(pd.DataFrame(nodes))   
    return nodes_dataframes


def create_nodes_df(nodes_dfs:list) -> pd.DataFrame:
    """
    Merges a list of dataframes to a dataframe
    """
    nodes = pd.concat(nodes_dfs).reset_index(drop=True)
    nodes['ON'] = nodes['ON'].apply(lambda x: None if x == [] else x) # remove empty lists
    nodes = nodes.drop_duplicates(subset=['NAME_NODE', 'LABEL_NODE', 'FILTER', 'FUNCTION']).reset_index(drop=True)
    nodes['ID'] = nodes.index
    nodes.to_csv('data/output-tables/nodes.csv') # save df

    return nodes





def reverse_subquery(query:dict) -> dict:
    """
    Reverse the order of the subqueries to access them from the deepest level
    """
    reversed_query= dict(reversed(list(query['subquery_dictionary'].items())))
    reversed_query['main_query'] = query['modified_SQL_query']
    
    return reversed_query



def parse_update_or_create_select_nodes(query, i):
    
    query_subqueries = reverse_subquery(query)

    filename = f"file_{i}"
    nodes = []

    for name_query in query_subqueries:
        ast = query_subqueries[name_query]

        select_statement_big = clean_query(query_subqueries[name_query])

        source_tables = []
        # for every select statement in query, extract the source tables, where expressions and on conditions
        for select in list(select_statement_big):
            source_table, where_exp, on_condition = get_statements(select) 
            source_tables += source_table            

        if 'subquery' in name_query: # if the query is a subquery then the name is the dict key, else the name is the target table
            nodes = add_node_subquery(nodes, name_query, filename, where_exp, on_condition)
        else:
            nodes, destination = add_node_mainquery(nodes, ast, where_exp, on_condition)
        nodes = add_node_sourcetables(nodes, source_tables, filename, on_condition)

    return nodes


def parse_update_or_create_set_nodes(query, query_node):
    nodes = []
    tables = []

    ast = query['modified_SQL_query']
    update = list(ast.find_all(exp.Update))[0]

    # join statements
    join  = list(ast.find_all(exp.Join))#[0]

    if join != []:
        for j in join:
            table = list(j.find_all(exp.Table))
            for t in table:
                tables.append({"table": t})
                nodes.append({'NAME_NODE': f"{str(t.db)}.{str(t.this)}",'LABEL_NODE': f"{str(t.db)}.{str(t.this)}", 'FILTER': None, 'FUNCTION': 'DataSources', 'ON': None, 'COLOR': "#42d6a4"})

    where_exp  = list(ast.find_all(exp.Where))
    if where_exp != []:
        where_exp = where_exp[0].sql('tsql')
    else:
        where_exp = None
    
    on_condition = "\n".join([i.sql('tsql') for i in list(ast.find_all(exp.Join))])

    target_db = str(list(ast.find_all(exp.Update))[0].this.db)
    target_node = str(list(ast.find_all(exp.Update))[0].this.this)
    destination=target_node

    #query_node = f"query_{target_node}"

    # source
    nodes.append({'NAME_NODE':f"{target_db}.{target_node}",'LABEL_NODE': f"{target_db}.{target_node}", 'FILTER': None, 'FUNCTION': 'DataSources', 'ON': None, 'COLOR': "#42d6a4"})
    # query
    nodes.append({'NAME_NODE': query_node,'LABEL_NODE': query_node, 'FILTER': where_exp, 'FUNCTION': 'query', 'ON': str(on_condition), 'COLOR': '#d0d3d3'})
    # target
    #nodes.append({'NAME_NODE': f"{target_db}.{target_node}",'LABEL_NODE': f"{target_db}.{target_node}", 'FILTER': None, 'FUNCTION': 'target', 'ON': on_condition, 'COLOR': "#42d6a4"})
    return nodes


def parse_declare_nodes(query):
    nodes = []
    variable = [i.replace('@', '') for i in re.findall(r"@\w+", query['modified_SQL_query'])][0]
    nodes.append({'NAME_NODE': variable,'LABEL_NODE': variable, 'FILTER': None, 'FUNCTION': 'variable', 'ON': None, 'COLOR': "#42d6a4"})



def extract_nodes(preprocessed_queries:list) -> pd.DataFrame:
    """
    Orchestrates the extraction of the nodes from a list of queries, the output being a nodes pd.DataFrame
    """

    # create nodes
    nodes_dfs = []
    queries= []

    for i, query in enumerate(preprocessed_queries):

        query_node = f"query_{i+1}"

        if query['type'] == 'update_or_create_select': # or  query['type'] == 'select' ### to add select without create or update
            nodes = parse_update_or_create_select_nodes(query, i)


        elif query['type'] == 'update_or_create_set':
            nodes = parse_update_or_create_set_nodes(query, query_node)


        elif query['type'] == 'declare':
            nodes = parse_declare_nodes(query)

        else:
            nodes = None


        if nodes != None:
            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)

    nodes = create_nodes_df(nodes_dfs)
    
    return nodes



#if __name__ == '__main__': #python -m modules.sql_parser.parse_nodes
#    from modules.sql_parser.extraction_sqlglot import preprocess_queries
#
#    preprocessed_queries = preprocess_queries('data/queries-txts/queries_rabo_qrm.txt') # 'data/queries-txts/WorldWideImporters 1.txt'
#
#    nodes = extract_nodes(preprocessed_queries)