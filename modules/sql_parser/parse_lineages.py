from sqlglot import parse_one, exp
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
import sqlglot



def find_table_w_spaces(tree: sqlglot.expressions):
    """
    Find all table names which have an empty space in them and storing them without the " " for later use, as sqlglot cannot parse them otherwise.
    """
    table_names = list(tree.find_all(exp.Table))
    space_table = []
    for element in table_names:
        if " " in element.name:
            space_table.append((element.name.replace(" ",""),element.name))
    return space_table


def extract_target_columns(tree: sqlglot.expressions.Select):
    """
    From the query in input, get all the columns from the select statement
    """
    # extract target columns
    select_statement_big = tree.find_all(exp.Select) # find all select statements

    select_statement = []
    for select in list(select_statement_big): # for every select statements, extract the columns
        select_statement += select.expressions 

    target_columns =[]
    for select in select_statement: # for every select statement, find all the target columns and add them to list
        columns = list(select.find_all(exp.Column))
        target_columns.append([i for i in columns])

    return select_statement, target_columns


# replace columns aliases
def transformer_functions(node):
    """
    Replaces column objects within the functions with simple column names
    """
    if isinstance(node, exp.Column):
        return parse_one(node.name)
    return node


def extract_transformation(tree: sqlglot.expressions.Select):
    """
    Function to extract possible transformation from columns
    """
    # add possible transformation to columns
    transformations = []

    for col in tree:
        if list(col.find_all(exp.Alias)) == []: # if there are no functions
            transformations.append("")
        else: # else add the function
            transformations.append(col.sql(dialect = "tsql"))

    return transformations


def split_at_last_as(input_string: str):  
    """
    Function to split transformation string at last " AS ", as everything after the last " AS " is the alias, not the transformation
    """
    split_point = input_string.rfind(' AS ')
    if split_point == -1:
        return input_string, ''
    return input_string[:split_point], input_string[split_point + 4:]


def extract_source_target_transformation(target_columns :list, lineages: list, space_table:list, target_node_name:str):
    """
    Function that returns a list of dictionaries, in which each dictionary contains the list of source columns, the target column and the possible transformation
    """
    for target_column in target_columns:
        source_columns = []

        for source_column in target_column[0]:

            #parse the table and column info
            table = source_column.table
            catalog = source_column.catalog
            db = source_column.db
            column = source_column.name

            for w in space_table:
                if table == w[0]:
                    table = w[1]

            if catalog !="" and db !="":
                source_column_complete = catalog + "." +  db +"." + table +"." +column

            elif catalog == "" and db == "":
                source_column_complete = table +"." +column
            elif catalog == "":    
                source_column_complete = db + "." + table + "."+column
            elif db == "":
                source_column_complete = catalog + "." + table +"." +column

            source_columns.append(source_column_complete)
                
        if source_columns != []:
            if 'AS' in target_column[1]: # if there is an alias, append formula and alias
                for col in source_columns:
                    if split_at_last_as(target_column[1])[0].strip() not in col:

                        lineages.append({'SOURCE_COLUMNS':source_columns, 'TARGET_COLUMN':f"{target_node_name}.{split_at_last_as(target_column[1])[1].strip()}", 'TRANSFORMATION':split_at_last_as(target_column[1])[0].strip()})
                    else:
                        lineages.append({'SOURCE_COLUMNS':source_columns, 'TARGET_COLUMN':f"{target_node_name}.{split_at_last_as(target_column[1])[1].strip()}", 'TRANSFORMATION': ""})
            else:

                lineages.append({'SOURCE_COLUMNS':source_columns, 'TARGET_COLUMN':f'{target_node_name}.{source_columns[0].split(".")[-1]}', 'TRANSFORMATION': target_column[1]})
    return lineages


def parse_table(table, table_alias_list, subquery=True):   
    """
    Function to parse all table information available (db, catalog...)
    """ 
    #table = element.this.this
    #table_name = table.name
    #catalog =  table.catalog
    #db = table.db

    if subquery == False:
        table_alias =  table.alias
        table_name = table.name
        table_db = table.db
        table_catalog = table.catalog

    else:
        table_alias = table.alias
        source = table.this.args["from"]
        table_name= source.this.name
        table_catalog =  source.this.catalog
        table_db = source.this.db
        
        
    if " " in table_name:
        table_name = table_name.replace(" ", "")

    if table_catalog == "":    
        result = (table_db+"."+table_name, table_alias)
    elif table_db == "":
        result = (table_catalog+"."+table_name, table_alias)
    else:
        result = (table_catalog+"."+ table_db+"."+table_name, table_alias)

    table_alias_list.append(result)
    return result