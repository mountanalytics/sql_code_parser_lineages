
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


def extract_subqueries(query):
        print(query)
        count = 0

        ast = parse_one(query, read="tsql")

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

        main_query = str(list(ast.find_all(exp.Create))[0])  

        # replace subqueries in main query
        for key, value in subqueries.items():
                
                main_query = main_query.replace(f"({str(value)})", str(key))
                main_query = main_query.replace(value, str(key))


        subqueries = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries}
        print(main_query)

        return subqueries


# open the sql queries that were generated with gpt
with open('data/queries-txts/TEST.txt', 'r') as file: 
    file = file.read().strip().split(';')
    sql_queries = [re.sub(r'\s+', ' ', query.strip().replace('\n', ' ').replace('\t', ' ')) for query in file if query.strip()]
    

# parse the subqueries and save a dictionary
    
for i, query in enumerate(sql_queries):
    subqueries = extract_subqueries(query)

    filename = f'data/preprocessed-queries/json_data{i}.json'

    # Save the dictionary as a JSON file
    with open(filename, 'w') as json_file:
        json.dump(subqueries, json_file, indent=4)

    print(f"Dictionary saved as {filename}")




