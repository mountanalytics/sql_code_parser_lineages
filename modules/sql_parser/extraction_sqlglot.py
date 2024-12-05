from sqlglot import parse_one, exp
from sqlglot.dialects.ma import MA
from sqlglot.dialects.tsql import TSQL
import sqlglot
import copy
import os
import json
import re



def replace_spaces_in_brackets(input_string: str, replacement: str = "_space_" ) -> str:
    """
    Replaces spaces within square brackets [] in a string with replacement using regex.

    :param input_string: The input string.
    :return: A new string with spaces replaced within square brackets.
    """
    def replace_space(match):
        # Replace spaces within the matched brackets with other sequence
        return match.group(0).replace(' ', replacement)
    
    # Regex pattern to find text within square brackets, including the brackets
    pattern = r'\[.*?\]'
    
    # Use re.sub with a replacement function
    return re.sub(pattern, replace_space, input_string)


def split_sql_string_to_sections(sql_string):
    # List of SQL keywords to identify the start of each section
    keywords = [
        r"DECLARE", r"SELECT", r"INSERT", r"UPDATE", r"DELETE", r"MERGE", r"CREATE", r"ALTER",
        r"DROP", r"TRUNCATE", r"BEGIN", r"DECLARE", r"EXEC(?:UTE)?", r"WITH",
        r"COMMIT", r"ROLLBACK", r"SAVEPOINT", r"USE", r"SHOW", r"DESCRIBE", r"EXPLAIN", r"WHILE"
    ]
    
    # Compile regex pattern to match any of the keywords at the start of a line
    keyword_pattern = re.compile(r"^\s*(" + "|".join(keywords) + r")\b", re.IGNORECASE)

    # Split the input string into lines
    sql_lines = sql_string.splitlines()

    sections = []
    current_section = []
    
    for line in sql_lines:
        # Check if the line starts with any of the keywords
        if keyword_pattern.match(line):
            # If there's an existing section, add it to the list
            if current_section:
                sections.append(" ".join(current_section))
                current_section = []
        # Add the current line to the current section
        current_section.append(line)

    # Add the last section if any
    if current_section:
        sections.append(" ".join(current_section))

    return [replace_spaces_in_brackets(re.sub(r'\s+', ' ', section)).replace('[', '').replace(']', '').strip() for section in sections] # remove multiple spaces and return



def open_queries(dir:str) -> list:
    """
    Open TSQL queries from one text file
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
    #ast = list(ast.find_all(exp.Select))[0]
    alias_table = get_tables(ast)
    
    def transformer_table(node):
        for element in alias_table:
            if isinstance(node, exp.Column) and node.table == element[1]:
                return parse_one(element[0] + "." + node.name)
        return node

    transformed_tree = ast.transform(transformer_table)

    return transformed_tree



def replace_spaces_in_brackets(input_string: str, replacement: str = "_space_" ) -> str:
    """
    Replaces spaces within square brackets [] in a string with replacement using regex.

    :param input_string: The input string.
    :return: A new string with spaces replaced within square brackets.
    """
    def replace_space(match):
        # Replace spaces within the matched brackets with other sequence
        return match.group(0).replace(' ', replacement)
    
    # Regex pattern to find text within square brackets, including the brackets
    pattern = r'\[.*?\]'
    
    # Use re.sub with a replacement function
    return re.sub(pattern, replace_space, input_string)


def preprocess_queries(dir:str) -> dict:
    """
    Orchestrates the preprocessing and extraction of the SQL queries
    """
    preprocessed_queries = []
    sql_queries = open_queries(dir)

    for i, query in enumerate(sql_queries):
  

        if 'declare' not in query.lower() and ('select' in query.lower() or 'set' in query.lower()):
            # parse
            #ast = sqlglot.parse_one(query, dialect = 'tsql')
            print(query)
            ast = parse_one(replace_spaces_in_brackets(query).replace('[', '').replace(']', ''))
            ast = replace_aliases(ast)

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


        if 'select' in query.lower() and ('update' in query.lower() or 'create' in query.lower()):
            
            preprocessed_query_json = {'modified_SQL_query': main_query.sql(), 'subquery_dictionary': subqueries_transformed_json, 'type': 'update_or_create_select'}
            save_preprocessed_query(preprocessed_query_json, i)
            preprocessed_query = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries_transformed, 'type': 'update_or_create_select'}
            preprocessed_queries.append(preprocessed_query)

        elif 'select' in query.lower() and not ('update' in query.lower() or 'create' in query.lower()):
            
            preprocessed_query_json = {'modified_SQL_query': main_query.sql(), 'subquery_dictionary': subqueries_transformed_json, 'type': 'select'}
            save_preprocessed_query(preprocessed_query_json, i)
            preprocessed_query = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries_transformed, 'type': 'select'}
            preprocessed_queries.append(preprocessed_query)

        elif ('update' in query.lower() and 'set' in query.lower()) or ('create' in query.lower() and 'set'in query.lower()) and 'select' not in query.lower():
            preprocessed_query_json = {'modified_SQL_query': main_query.sql(), 'subquery_dictionary': subqueries_transformed_json, 'type': 'update_or_create_set'}
            save_preprocessed_query(preprocessed_query_json, i)
            preprocessed_query = {'modified_SQL_query': main_query, 'subquery_dictionary': subqueries_transformed, 'type': 'update_or_create_set'}
            preprocessed_queries.append(preprocessed_query)

        elif 'declare' in query.lower():
            preprocessed_query_json = {'modified_SQL_query': query, 'subquery_dictionary': '', 'type': 'declare'}
            save_preprocessed_query(preprocessed_query_json, i)
            preprocessed_query = {'modified_SQL_query': query, 'subquery_dictionary': '', 'type': 'declare'}
            preprocessed_queries.append(preprocessed_query)

        else:
            preprocessed_query_json = {'modified_SQL_query': query, 'subquery_dictionary': '', 'type': 'other'}
            save_preprocessed_query(preprocessed_query_json, i)
            preprocessed_query = {'modified_SQL_query': query, 'subquery_dictionary': '', 'type': 'other'}
            preprocessed_queries.append(preprocessed_query)





    return preprocessed_queries


if __name__ == '__main__':
    preprocess_queries('data/queries-txts/queries_rabo_qrm.txt')

