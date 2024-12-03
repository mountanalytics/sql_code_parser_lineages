from sqlglot import parse_one, exp
from sqlglot.dialects.ma import MA
from sqlglot.dialects.tsql import TSQL
import sqlglot
import copy
import os
import json
import re


def open_query(dir:str) -> list:
    """
    Open TSQL queries from one text file, every element of the returned list is a query (split at ";")
    """ 
    with open(dir, 'r') as file: 
        file = file.read().strip().split(';')
        sql_queries = [re.sub(r'\s+', ' ', query.strip().replace('\n', ' ').replace('\t', ' ')) for query in file if query.strip()]
    return sql_queries


def extract_subqueries(ast: sqlglot.expressions) -> dict:
    """
    Extract all subqueries and nested subqueries from a TSQL query and saves them in a dictionary with structured format
    """
    count = 0
    selects = list(ast.find_all(exp.Select))   
    selects = [select for select in selects] # problem with tsql conversion: column aliases get dropped in case of no transformation
    subqueries = {}
    
    for i, select in enumerate(selects):
        subqueries[f'subquery_{i}'] = select

    subqueries = {k: v for k, v in subqueries.items() if k != 'subquery_0'}
    return subqueries


def replace_subquery_with_table_in_main_query(ast: sqlglot.expressions, subqueries: dict) -> sqlglot.expressions:
    """
    Wrapper to transform the queries (replacing subqueries with a table name)
    """

    def replace_subquery_with_table(node):
        """
        Transformer function to replace subqueries with table names (subquery_1, subquery_2...)
        """
        if type(node) == sqlglot.expressions.Select:
            for name, subquery in subqueries.items():    
                if node.sql() == subquery.sql():  # Check if the node is a subquery
                    return sqlglot.exp.Table(this=name)  # Replace with a table node
        return node
    
    transformed = ast.transform(replace_subquery_with_table)

    return transformed




def replace_subquery_with_table_in_subqueries(node: sqlglot.expressions, subqueries: dict) -> sqlglot.expressions:
    """
    Wrapper to transform the queries (replacing nested subqueries with a table name)
    """

    def extract_subqueries_from_subquery(ast: sqlglot.expressions.Select) -> dict:
        """
        Extract all subqueries from a query and saves them in a dictionary with structured format
        """
        count = 0
        selects = list(ast.find_all(exp.Select))   
        selects = [select for select in selects] # problem with tsql conversion: column aliases get dropped in case of no transformation
        subqueries = {}
        
        for i, select in enumerate(selects):
            subqueries[f'subquery_{i}'] = select
        subqueries = {k: v for k, v in subqueries.items() if k != 'subquery_0'}

        return subqueries
    
    def replace_subquery_with_table(node: sqlglot.expressions) -> sqlglot.expressions:
        """
        Transformer function to replace subqueries with table names (subquery_1, subquery_2...)
        """
        if type(node) == sqlglot.expressions.Select:
            for name, subquery in subqueries.items():    
                if node.sql() == subquery.sql():  # Check if the node is a subquery
                    return sqlglot.exp.Table(this=name)  # Replace with a table node
        return node
    

    subqueries_main = subqueries.copy()
    nested_subqueries = extract_subqueries_from_subquery(node)

    subqueries = {}

    # in a subquery, replace the name of the nested subqueries with the name in the origin extracted subqueries dictionary
    for key1, value1 in subqueries_main.items():
        for key2, value2 in nested_subqueries.items():
            if value1 == value2:
                subqueries[key1] = value1

    transformed = node.transform(replace_subquery_with_table)

    return transformed


def save_preprocessed_query(preprocessed_query: dict, idx: int):
    """
    Save parsed query in json file for debugging
    """
    filename = f'data/preprocessed-queries/json_data{idx}.json'
    with open(filename, 'w') as json_file:
        json.dump(preprocessed_query, json_file, indent=4)



def preprocess_queries(dir:str) -> dict:
    """
    Orchestrates the preprocessing and extraction of the SQL queries
    """
    preprocessed_queries = []
    sql_queries = open_query(dir)

    for i, query in enumerate(sql_queries):
        # parse
        ast = sqlglot.parse_one(query, dialect = 'tsql')

        subqueries = extract_subqueries(ast)

        # extract subqueries from main query
        main_query = replace_subquery_with_table_in_main_query(ast, subqueries)

        subqueries_transformed = {}
        subqueries_transformed_json = {}

        # extract subqueries from subqueries
        for name, subquery in subqueries.items():
            subquery_transformed = replace_subquery_with_table_in_subqueries(subquery, subqueries)
            subqueries_transformed[name] = subquery_transformed
            subqueries_transformed_json[name] = subquery_transformed.sql()

        preprocessed_query_json = {'modified_SQL_query': main_query.sql(), 'subquery_dictionary': subqueries_transformed_json}
        save_preprocessed_query(preprocessed_query_json, i)

        preprocessed_query = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries_transformed}
        preprocessed_queries.append(preprocessed_query)

    return preprocessed_queries


if __name__ == '__main__':
    preprocess_queries('data/queries-txts/queries_rabo_qrm.txt')

