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


def parse_query(query: str):
    """
    Function to convert query string to a sqlglot parsed tree
    """
    ast = parse_one(query, read="tsql")
    trial1 = repr(ast)
    return ast


# parse table name + table alias
def parse_tables(table, table_alias_list, subquery=True):    
    """
    Function to parse all table information available (db, catalog...)
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
    Function to extract the table names and their aliases, used to reconstruct a tuple with structure (database+schema+name, alias )
    """
    # find all tables
    table_alias = list(ast.find_all(exp.Table))
    alias_table = []

    # extract information from each table
    for table in table_alias:
        parse_tables(table, alias_table, False)

    return alias_table


def replace_aliases(query:str):
    ast = parse_query(query)

    ast = list(ast.find_all(exp.Select))[0]

    alias_table = get_tables(ast)
    
    def transformer_table(node):
        for element in alias_table:
            if isinstance(node, exp.Column) and node.table == element[1]:
                return parse_one(element[0] + "." + node.name)
        return node

    transformed_tree = ast.transform(transformer_table)

    return transformed_tree

def extract_from_statements(tree, source_tables):
    from_exp = list(tree.find_all(exp.From))
    from_table =str(from_exp[0].this).split(' AS')[0] # table
    source_tables.append(from_table)
    return source_tables

def extract_join_statements(tree, source_tables):
    join_exp = list(tree.find_all(exp.Join))
    if join_exp != []:
        join_table = str(join_exp[0].this).split(' AS')[0] # table
        source_tables.append(join_table)
    else:
        join_exp = None
    return source_tables


def extract_where_statements(tree):
    where_exp = list(tree.find_all(exp.Where))
    if where_exp != []:
        where_exp = str(where_exp[0].this).split(' AS')[0]# table
    else:
        where_exp = None
    return where_exp


def extract_on_statements(tree):
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


def get_statements(transformed_tree):
    """
    Function to extract from expression, join expression and where expression from query
    """

    source_tables = []
    # from expression
    source_tables = extract_from_statements(transformed_tree, source_tables)

    # join expression
    source_tables = extract_join_statements(transformed_tree, source_tables)

    # where expression
    where_exp = extract_where_statements(transformed_tree)

    # join expressions
    on_exp = extract_on_statements(transformed_tree)

    return source_tables, where_exp, on_exp



