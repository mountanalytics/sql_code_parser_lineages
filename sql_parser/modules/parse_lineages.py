from sqlglot import parse_one, exp
from sqlglot.dialects.tsql import TSQL
import sqlglot
import pandas as pd
import os
import json
import re
from .parse_nodes import *


def find_table_w_spaces(tree: sqlglot.expressions) -> list:
    """
    Finds all table names which have an empty space in them and storing them without the " " for later use, as sqlglot cannot parse them otherwise.
    """
    table_names = list(tree.find_all(exp.Table))
    space_table = []
    for element in table_names:

      
        try:
            if " " in element.name:
                space_table.append((element.name.replace(" ",""),element.name))
        except AttributeError:
            pass

    return list(set(space_table)) # a list of tuples with table names paired (space removed original - original) Eg. (OrderDetails, Order Details)


def extract_target_columns(tree: sqlglot.expressions.Select) -> tuple[list, list]:
    """
    Extract all the columns from the select statement in the input query
    """

    #print(repr(tree))
    # extract target columns
    select_statement_big = tree.find_all(exp.Select) # find all select statements

    expressions = []
    for i, select in enumerate(list(select_statement_big)): # for every select statements, extract the columns
        expressions += select.expressions 

    target_columns =[]
    for select in expressions: # for every select statement, find all the target columns and add them to list
        columns = list(select.find_all(exp.Column))
        if columns == []:
            columns.append(str(select))

        target_columns.append([i for i in columns])
        #target_columns.append(select)


    return expressions, target_columns


def transformer_functions(node: sqlglot.expressions.Select) -> sqlglot.expressions.Select:
    """
    Replaces column objects within the functions with simple column names
    """
    if isinstance(node, exp.Column):
        return parse_one(node.name)
    return node


def extract_transformation(tree: sqlglot.expressions.Select) -> list:
    """
    Extract possible transformation from columns
    """
    # add possible transformation to columns
    transformations = []

    for col in tree:
        if list(col.find_all(exp.Alias)) == []: # if there are no functions
            transformations.append("")
        else: # else add the function
            transformations.append(col.sql())

    return transformations


def split_at_last_as(input_string: str)-> str:  
    """
    Splits transformation string at last " AS ", as everything after the last " AS " is the alias, not the transformation
    """
    split_point = input_string.rfind(' AS ')
    if split_point == -1:
        return input_string, ''
    return input_string[:split_point], input_string[split_point + 4:]


def get_next_nodes(query:dict, component:str, destination:str)-> tuple[str, str]:
    """
    Extracts the next node in line and the query node
    """
    # next node in case of subquery in subquery, subquery in main_query or main_query
    if "subquery" in component:
        for key, value in query.items():
            if component in value.sql(): # if the component is in the query 
                if 'subquery' in key: # subquery in subquery
                    query_node=component
                    target_node = key
                else: # subquery in main_query
                    query_node=component
                    target_node = f'query_{destination}'
    else: # main_query
        query_node = f'query_{destination}'
        target_node =  destination

    return query_node, target_node



def create_lineages_df(lineages:list, nodes:pd.DataFrame,  filename:str, destination:str) -> pd.DataFrame:
    """
    Converts the lineages list of dictionaries to a pd.DataFrame and merges the results with the nodes id
    """

    lineages = pd.DataFrame(lineages)
    lineages['FILE_NAME'] = filename
    lineages['ROW_ID'] = 0
    lineages['LINK_VALUE'] = 1
    lineages['SOURCE_FIELD'] = lineages['SOURCE_COLUMNS'].str.extract(r'\[([^\]]*)\]')
    lineages['TARGET_FIELD'] = lineages['TARGET_COLUMN'].str.extract(r'\[([^\]]*)\]')

    lineages['SOURCE_NODE'] = lineages['SOURCE_COLUMNS'].str.split('[', expand=True)[0]
    lineages['TARGET_NODE'] = lineages['TARGET_COLUMN'].str.split('[', expand=True)[0]
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

    try:
        lineages.to_csv(f"data/output-tables/lineages/lineage-{destination.replace('.', '_')}.csv") # save csv
    except:
        print('Cannot save lineages dataset')

    return lineages



def extract_lineage(lineages:list, target_columns:list, query_node:str, target_node:str) -> list:
    """
    Extracts the lineages from a list of columns (source, target and transformation)
    """

    for column in target_columns:
        for source_col in column[0]:

            if type(source_col)!=str:
            
                # if there is a database name in the column
                if source_col.db!="":
                    db = source_col.db + "."
                else:
                    db = ""


                if 'subquery' in source_col.table:
                

                    if column[1] == '': # add lineage with no transformation
                        lineages.append({'SOURCE': f"{query_node}[{source_col.this}]", 'TARGET': f"{target_node}[{source_col.this}]", 'TRANSFORMATION': column[1]})
                    else: # add lineage with transformation
                        lineages.append({'SOURCE': f"{query_node}[{[source_col_i.this.this for source_col_i in column[0]]}]", 'TARGET': f"{target_node}[{column[1].split('AS')[1].strip()}]", 'TRANSFORMATION': column[1].split('AS')[0].strip()})
                else:   
                    if column[1] == '': # add lineage with no transformation
                        lineages.append({'SOURCE': f"{db}{source_col.table}[{source_col.this}]", 'TARGET': f"{query_node}[{source_col.this}]", 'TRANSFORMATION': ""})
                        lineages.append({'SOURCE': f"{query_node}[{source_col.this}]", 'TARGET': f"{target_node}[{source_col.this}]", 'TRANSFORMATION': column[1]})
                    else: # add lineage with transformation
                        lineages.append({'SOURCE': f"{db}{source_col.table}[{source_col.this}]", 'TARGET': f"{query_node}[{source_col.this}]", 'TRANSFORMATION': ""})
                        lineages.append({'SOURCE': f"{query_node}[{[source_col_i.this.this for source_col_i in column[0]][0]}]", 'TARGET': f"{target_node}[{column[1].split('AS')[1].strip()}]", 'TRANSFORMATION': column[1].split('AS')[0].strip()})
            else:
                

                lineages.append({'SOURCE': f"{query_node}[{source_col}]", 'TARGET': f"{target_node}[{source_col}]", 'TRANSFORMATION': source_col})


    return lineages


def extract_lineages(preprocessed_queries:list, nodes:pd.DataFrame, node_name :str) -> list:
    """
    Orchestrates the extraction of the lineages from each query, the result is a list of pd.DataFrame, one for each query
    """

    lineages_dfs = []
    lineages = []
    #reversed_preprocessed_queries = reverse_subqueries(preprocessed_queries)

    for i, query in enumerate(preprocessed_queries): # for query in queries
        query_node = f"query_{node_name}_{i+1}"
        if query['type'] == 'update_or_create_select': # or  query['type'] == 'select' ### to add select without create or update

            query = reverse_subquery(query)

            filename = node_name
            try:
                destination =  list(query['main_query'].find_all(exp.Create))[0].this.this.this
            except:
                destination =  str(list(query['main_query'].find_all(exp.Insert))[0].this.this)

            # insert into try except goes here
            for component in query.keys(): # for query component (sub/main queries) in query
                print(component)
                target_columns =[]
                # preprocess query and extract all useful information
                ast = query[component]
                space_table = find_table_w_spaces(ast) # list with tables with spaces (sqlglot cant parse them)
                alias_table = get_tables(ast) # parse table name + table alias
                tree = replace_aliases(query[component]) # remove table aliases
                
                select_statement, target_columns = extract_target_columns(tree) # extract target columns
                select_statement = [x.transform(transformer_functions) for x in select_statement] # remove column aliases
                transformations = extract_transformation(select_statement)
                target_columns = list(zip(target_columns, transformations)) 
                query_node, target_node = get_next_nodes(query, component, destination)
                lineages = extract_lineage(lineages, target_columns, query_node, target_node)

                try:
                    where = list(ast.find_all(exp.Where))[0]
                    variables = list(where.find_all(exp.Var))

                    for variable in variables: 
                        lineages.append({'SOURCE_COLUMNS':f'@{variable}[{variable}]', 'TARGET_COLUMN':f"{query_node}[{variable}]", 'TRANSFORMATION':""})

                except:
                    pass
                
        elif query['type'] == 'update_or_create_set':
            filename = f"file_{i}"


            #lineages = []

            ast = replace_aliases(query['modified_SQL_query'])
            ast = replace_alias_update_table(ast)

            update = list(ast.find_all(exp.Update))[0]

            destination_db =  str(list(ast.find_all(exp.Update))[0].this.db)
            destination =  str(list(ast.find_all(exp.Update))[0].this.this)

            tables = []

            columns = list(update.expressions)

            for column in list(update.expressions):     

                target_col =str(list(column.this.find_all(exp.Column))[0]) if len(list(column.this.find_all(exp.Column))) == 1 else None 
                source_col = str(list(column.expression.find_all(exp.Column))[0]) if len(list(column.expression.find_all(exp.Column))) == 1 else None
                transformation = str(column.expression) if str(column.expression) != source_col else ""
                value = str(list(column.expression.find_all(exp.Literal))[0]) if len(list(column.expression.find_all(exp.Literal))) == 1 else None

                
                if source_col != None:
                    lineages.append({'SOURCE': f"{'.'.join(source_col.split('.')[:-1])}[{source_col.split('.')[-1]}]", 'TARGET': f"{query_node}[{source_col.split('.')[-1]}]", 'TRANSFORMATION': ""})
                    lineages.append({'SOURCE': f"{query_node}[{source_col.split('.')[-1]}]", 'TARGET': f"{'.'.join(target_col.split('.')[:-1])}[{target_col.split('.')[-1]}]", 'TRANSFORMATION': transformation})
                else:
                    value = f"value: {value}"

                    lineages.append({'SOURCE': f"{query_node}[{value}]", 'TARGET': f"{'.'.join(target_col.split('.')[:-1])}[{target_col.split('.')[-1]}]", 'TRANSFORMATION': ""})

                from_statement = list(update.find_all(exp.From))

        elif query['type'] == 'insert_into':
            filename = f"file_{i}"

            #lineages = []
            table = list(query['modified_SQL_query'].find_all(exp.Table))[0]
            destination = table

            target_columns = [str(i) for i in list(query['modified_SQL_query'].find_all(exp.Schema))[0].expressions]
            source_values = [str(i) for i in list(query['modified_SQL_query'].find_all(exp.Tuple))[0].expressions]

            for idx, variable in enumerate(source_values):
                if "::" in variable:
                    lineages.append({'SOURCE': f"{variable}[{variable}]", 'TARGET': f"{query_node}[{variable}]", 'TRANSFORMATION': ""})
                    lineages.append({'SOURCE': f"{query_node}[{variable}]", 'TARGET': f"{table}[{target_columns[idx]}]", 'TRANSFORMATION': ""})

                else:
                    lineages.append({'SOURCE': f"{query_node}[{variable}]", 'TARGET': f"{table}[{target_columns[idx]}]", 'TRANSFORMATION': ""})

        elif query['type'] == 'while_delete':
            pass
            #lineages = []

     

        #try:
#
        #    lineages_df = create_lineages_df(lineages, nodes, filename, destination)
        #    lineages_dfs.append(lineages_df)
        #except:
        #    return None
    

    return lineages