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
    Open sql queries from one text file
    """ 
    with open(dir, 'r') as file: 
        file = file.read().strip().split(';')
        sql_queries = [re.sub(r'\s+', ' ', query.strip().replace('\n', ' ').replace('\t', ' ')) for query in file if query.strip()]
    return sql_queries


def extract_subqueries(ast: sqlglot.expressions) -> dict:
    """
    Extract all subqueries from a query and saves them in a dictionary with structured format
    """
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

    return subqueries


def replace_nested_subqueries_in_subqueries(subqueries: dict) -> dict:
    """
    Replace nested subqueries in subqueries with the key
    """
        
    for i, query_i in subqueries.items():
        for j, query_j in subqueries.items():
            if j in query_i:
                subqueries[i] = subqueries[i].replace(j, query_j)
    return subqueries


def replace_subqueries_in_mainquery(ast: sqlglot.expressions, subqueries_global:dict) -> str:
    """
    Replace nested subqueries in main_query with the key
    """
    subqueries = subqueries_global.copy()
    try:
        main_query = str(list(ast.find_all(exp.Create))[0])  
    except:
        main_query = str(list(ast.find_all(exp.Insert))[0])

    subqueries = replace_nested_subqueries_in_subqueries(subqueries)

    for key_i, value_i in subqueries.items():
            
        main_query = main_query.replace(f"({str(value_i)})", str(key_i))
        main_query = main_query.replace(value_i, str(key_i))

    return main_query


def save_preprocessed_query(preprocessed_query, idx):
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
        ast = parse_one(query, read="ma")
        subqueries = extract_subqueries(ast)
        main_query = replace_subqueries_in_mainquery(ast, subqueries)
        preprocessed_query = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries}
        preprocessed_queries.append(preprocessed_query)

    return preprocessed_queries