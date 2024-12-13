from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import sqlglot
import pandas as pd
import configparser
import os
import json
import re


def parse_query(query: str) -> sqlglot.expressions:
    """
    Parses to convert query string to a sqlglot parsed tree
    """
    ast = parse_one(query)
    trial1 = repr(ast)
    return ast


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
    try:
        ast = list(ast.find_all(exp.Select))[0]
    except IndexError:
        pass
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
    #ast = list(ast.find_all(exp.Select))[0]
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
        for join in join_exp:
            join_table = str(join.this).split(' AS')[0] # table
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
        where_exp = str(where_exp[0].this.sql('tsql')).split(' AS')[0]# table
        #where_exp = sql_to_natural_language(str(where_exp[0].this.sql('tsql'))).split(' AS')[0]# table

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
            on_conditions.append(join.sql('tsql'))
            #on_conditions.append(f"{list(join.find_all(exp.EQ))[0].this.table}.{list(join.find_all(exp.EQ))[0].this.this} = {list(join.find_all(exp.EQ))[0].expression.table}.{list(join.find_all(exp.EQ))[0].expression.this}")
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
    #if query.startswith("("): # remove open and closing paranthesis from subqueries
    #    query = query.strip("()")
    #else:
    #    pass
    #ast = parse_query(query) # get parsed tree
    tree = replace_aliases(ast) # get transformed tree without table aliases
    select_statement_big = tree.find_all(exp.Select) # parse selects before getting statements
    return select_statement_big


def add_node_subquery(nodes:list, query_name:str, file_name:str, where_exp:str, on_condition:str) -> list:
    """
    Add a node to the nodes list
    """
    target_node = query_name
    #nodes.append({'NAME_NODE': f"{target_node}", 'LABEL_NODE': f'{file_name}@{target_node}', 'FILTER': where_exp, 'FUNCTION': 'subquery', 'JOIN_ARG': on_condition})
    nodes.append({'NAME_NODE': target_node, 'LABEL_NODE': f'{file_name}@{target_node}', 'FILTER': where_exp, 'FUNCTION': 'subquery', 'JOIN_ARG': on_condition, 'COLOR': '#d0d3d3'})
    return nodes


def add_node_mainquery(nodes:list, ast: sqlglot.expressions, where_exp, on_condition) -> list:
    """
    Add main query node to the nodes list
    """

    #print(str(list(ast.find_all(exp.Select))[0].expressions[0]).split('(')[0])


    try: # try to find the create or insert into statements
        target_node = list(ast.find_all(exp.Create))[0].this.this.this
        nodes.append({'NAME_NODE': f"query_{target_node}",'LABEL_NODE': f"query_{target_node}", 'FILTER': where_exp, 'FUNCTION': 'query', 'JOIN_ARG': on_condition, 'COLOR': '#d0d3d3'})
        nodes.append({'NAME_NODE': target_node,'LABEL_NODE': target_node, 'FILTER': None, 'FUNCTION': 'target', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})
    except IndexError:
        target_node = list(ast.find_all(exp.Insert))[0].this.this.this
        nodes.append({'NAME_NODE': f"query_{target_node}",'LABEL_NODE': f"query_{target_node}", 'FILTER': where_exp, 'FUNCTION': 'query', 'JOIN_ARG': on_condition, 'COLOR': '#d0d3d3'})
        nodes.append({'NAME_NODE': target_node,'LABEL_NODE': target_node, 'FILTER': None, 'FUNCTION': 'target', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})
    return nodes


def add_node_sourcetables(nodes:list, source_tables:list, file_name:str, on_condition:list) -> list:
    """
    Add source tables to nodes list
    """
    for table in source_tables:
        #if table not in [node['NAME_NODE'] for node in nodes]:
            if 'subquery' in table: 
                nodes.append({'NAME_NODE': table,'LABEL_NODE': f'{file_name}@{table}', 'FILTER': None, 'FUNCTION': 'subquery', 'JOIN_ARG': on_condition, 'COLOR': '#d0d3d3'})
            else:
                nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})
    return nodes


def append_convert_nodes_to_df(nodes_dataframes:list, nodes:list) -> pd.DataFrame:
    """
    Converts a node (list of dictionaries) to dataframe and appends it to list
    """
    nodes = pd.DataFrame(nodes)

    nodes['COLOR'] = nodes.apply(
        lambda row: row['COLOR'] if row['FILTER'] == None else '#db59a5',
        axis=1
    )

    nodes['COLOR'] = nodes.apply(
        lambda row: row['COLOR'] if (row['JOIN_ARG'] == None and row['COLOR'] != '#db59a5') else '#db59a5',
        axis=1
    )

    nodes_dataframes.append(pd.DataFrame(nodes))   
    return nodes_dataframes


def create_nodes_df(nodes_dfs:list) -> pd.DataFrame:
    """
    Merges a list of dataframes to a dataframe
    """
    nodes = pd.concat(nodes_dfs).reset_index(drop=True)
    nodes['JOIN_ARG'] = nodes['JOIN_ARG'].apply(lambda x: None if x == [] else x) # remove empty lists
    nodes = nodes.drop_duplicates(subset=['NAME_NODE', 'LABEL_NODE', 'FILTER', 'JOIN_ARG']).reset_index(drop=True)
    nodes['ID'] = nodes.index

    try:
        nodes.to_csv('data/output-tables/nodes.csv') # save df
    except OSError:
        print('Cannot save nodes dataset')

    return nodes



def reverse_subquery(query:dict) -> dict:
    """
    Reverse the order of the subqueries to access them from the deepest level
    """
    reversed_query= dict(reversed(list(query['subquery_dictionary'].items())))
    reversed_query['main_query'] = query['modified_SQL_query']
    
    return reversed_query


def replace_alias_update_table(ast: sqlglot.expressions) -> sqlglot.expressions:
    """
    Replaces the tables' aliases in a query
    """
    update_table = list(ast.find_all(exp.Update))[0].this#.find_all(exp.Table)
    #print(repr(list(ast.find_all(exp.Update))[0]))
    try:
        from_tables = list(list(ast.find_all(exp.From))[0].find_all(exp.Table))[0]
        db = from_tables.db
        table = from_tables.this
        alias = from_tables.alias

        # if alias == update_table:
        ast = ast.transform(lambda node: sqlglot.exp.Table(this=f"{db}.{table}") if node.this == alias else node)
        
    except:
        pass

    return ast


def sql_to_natural_language(sql_where_clause):
    """
    Converts a SQL WHERE clause into a natural language explanation.

    Parameters:
        sql_where_clause (str): The SQL WHERE clause to be translated.

    Returns:
        str: The natural language explanation.
    """
    # Replace common SQL syntax with natural language equivalents
    replacements = [
        (r"\bAND\b", "and"),
        (r"\bOR\b", "or"),
        (r"=", "equals"),
        (r"IN \((.*?)\)", r"is one of \1"),
        (r"LIKE '%(.*?)%'", r"contains '\1'"),
        (r"LIKE '(.*?)%'", r"starts with '\1'"),
        (r"LIKE '%(.*?)'", r"ends with '\1'"),
        (r"\(\s*(.*?)\s*\)", r"(\1)")  # Remove extra spaces inside parentheses
    ]
    
    natural_lang = sql_where_clause.strip()
    for pattern, replacement in replacements:
        natural_lang = re.sub(pattern, replacement, natural_lang, flags=re.IGNORECASE)

    # Add a period after OR conditions for better readability
    groups = re.split(r"\\s*\\bOR\\b\\s*", natural_lang, flags=re.IGNORECASE)
    explanation = []
    
    for group in groups:
        # Keep AND intact in the explanation
        readable_group = re.sub(r"\\s*\\bAND\\b\\s*", " and ", group)
        explanation.append(f"{readable_group.strip()}")
    
    # Rejoin with " or "
    return " or ".join(explanation)


def get_variable_tables(ast, variable_tables):

    def extract_target_columns(ast: sqlglot.expressions.Select):
        """
        From the query in input, get all the columns from the select statement
        """
        # extract target columns
        select_statement_big = ast.find_all(exp.Select) # find all select statements

        select_statement = []
        for select in list(select_statement_big): # for every select statements, extract the columns
            select_statement += select.expressions 

        target_columns = []
        for select in select_statement: # for every select statement, find all the target columns and add them to list
            columns = list(select.find_all(exp.Column))
            target_columns.append([i for i in columns])

        return select_statement, target_columns
    
    _, target_columns = extract_target_columns(ast)
    
    insert_tables = [table.this.this.this for table in ast.find_all(exp.Insert)]
    insert_tables += [table.this.this.this for table in ast.find_all(exp.Into)]



    for table in insert_tables:     
        if '_doublecolumns_' in table: # if result table is a variable
            variable_tables[table] = [(col[0].this.this, i) for i, col in enumerate(target_columns)]

    return variable_tables

def extract_nodes(preprocessed_queries:list, node_name:str, variable_tables:dict) -> pd.DataFrame:
    """
    Orchestrates the extraction of the nodes from a list of queries, the output being a nodes pd.DataFrame
    """

    #reversed_preprocessed_queries = reverse_subqueries(preprocessed_queries)
    # create nodes
    nodes = []
    nodes_dfs = []
    queries= []


    for i, query in enumerate(preprocessed_queries):

        query_node = f"query_{node_name}_{i+1}"

        if query['type'] == 'update_or_create_select': # or  query['type'] == 'select' ### to add select without create or update


            query_subqueries = reverse_subquery(query)

            filename = node_name
            #nodes = []
            for name_query in query_subqueries:


                ast = query_subqueries[name_query]

                variable_tables= get_variable_tables(ast, variable_tables)

                # CLEAN QUERY
                select_statement_big = clean_query(query_subqueries[name_query])

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


        elif query['type'] == 'update_or_create_set':


            def split_on_and_or(input_string):
                # Define a pattern to match "and" or "or" and capture the surrounding text
                pattern = r'(\b(?:and|or)\b)'
                
                # Use re.split to include delimiters (and/or) in the result
                parts = re.split(pattern, input_string, flags=re.IGNORECASE)
                
                result = []
                buffer = ""
                
                for part in parts:
                    stripped = part.strip()
                    if stripped.lower() in {"and", "or"}:
                        # When encountering "and" or "or", save the current buffer
                        if buffer:
                            result.append(buffer.strip())
                            buffer = ""
                        buffer = stripped
                    else:
                        # Append the current part to the buffer
                        if buffer:
                            buffer += " " + stripped
                        else:
                            buffer = stripped
                
                if buffer:
                    result.append(buffer.strip())
                
                return result

            tables = []

            ast = query['modified_SQL_query']
            ast =replace_alias_update_table(ast)
            update = list(ast.find_all(exp.Update))[0]

            # join statements
            join  = list(ast.find_all(exp.Join))#[0]

            if join != []:
                for j in join:
                    table = list(j.find_all(exp.Table))
                    for t in table:
                        tables.append({"table": t})
                        nodes.append({'NAME_NODE': f"{str(t.db)}.{str(t.this)}",'LABEL_NODE': f"{str(t.db)}.{str(t.this)}", 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})

            where_exp  = list(ast.find_all(exp.Where))
            if where_exp != []:
                where_exp =  split_on_and_or(sql_to_natural_language(where_exp[0].sql('tsql')))#.split('AND')
            else:
                where_exp = None
            
            on_condition = split_on_and_or("\n".join([i.sql('tsql') for i in list(ast.find_all(exp.Join))]))

            target_db = str(list(ast.find_all(exp.Update))[0].this.db)+ "." if str(list(ast.find_all(exp.Update))[0].this.db)!="" else ""
            target_node = str(list(ast.find_all(exp.Update))[0].this.this)
            destination=target_node

            # source
            nodes.append({'NAME_NODE':f"{target_db}{target_node}",'LABEL_NODE': f"{target_db}{target_node}", 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': "#42d6a4"})
            # query
            nodes.append({'NAME_NODE': query_node,'LABEL_NODE': query_node, 'FILTER': where_exp, 'FUNCTION': 'query', 'JOIN_ARG': str(on_condition), 'COLOR': '#d0d3d3'})
            # target
            #nodes.append({'NAME_NODE': f"{target_db}.{target_node}",'LABEL_NODE': f"{target_db}.{target_node}", 'FILTER': None, 'FUNCTION': 'target', 'JOIN_ARG': on_condition, 'COLOR': "#42d6a4"})
            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)

        elif query['type'] == 'while_delete':

            ast = query['modified_SQL_query']
            where_exp = list(ast.find_all(exp.Where))
            tables = list(ast.find_all(exp.Table))

            nodes.append({'NAME_NODE': str(tables[1]),'LABEL_NODE': str(tables[1]), 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})
            nodes.append({'NAME_NODE': query_node,'LABEL_NODE': query_node, 'FILTER': 'DELETE ' + sql_to_natural_language(where_exp[0].sql('tsql')), 'FUNCTION': 'query', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})

            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)

        elif query['type'] == 'declare':

            variable = query['modified_SQL_query'].split("=")[1].strip()
 
            nodes.append({'NAME_NODE': variable,'LABEL_NODE': variable, 'FILTER': None, 'FUNCTION': 'variable', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})
            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)
        
        elif query['type'] == 'if_not_exists':

            variable = re.findall(r'@\w+', query['modified_SQL_query'])[-1]

            nodes.append({'NAME_NODE': variable,'LABEL_NODE': variable, 'FILTER': query['modified_SQL_query'], 'FUNCTION': 'query', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})
            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)
                
        elif query['type'] == 'insert_into':

            table = list(query['modified_SQL_query'].find_all(exp.Table))[0]

            target_columns = [str(i) for i in list(query['modified_SQL_query'].find_all(exp.Schema))[0].expressions]
            variables = [str(i) for i in list(query['modified_SQL_query'].find_all(exp.Tuple))[0].expressions]

            variables = []
            for variable in list(query['modified_SQL_query'].find_all(exp.Tuple))[0].find_all(exp.Literal):
                if '::' in str(variable):
                    nodes.append({'NAME_NODE': str(variable),'LABEL_NODE': str(variable), 'FILTER': None, 'FUNCTION': 'variable', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})

            nodes.append({'NAME_NODE': table,'LABEL_NODE': table, 'FILTER': None, 'FUNCTION': 'DataSources', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})
            nodes.append({'NAME_NODE': query_node,'LABEL_NODE': query_node, 'FILTER': 'None', 'FUNCTION': 'query', 'JOIN_ARG': None, 'COLOR': '#d0d3d3'})
        
            nodes_dfs = append_convert_nodes_to_df(nodes_dfs, nodes)


    #nodes_df = create_nodes_df(nodes_dfs)

    return nodes, variable_tables



