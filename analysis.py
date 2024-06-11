import os
import pandas as pd
import ast
import numpy as np


def calculate_grade(strings_list: list, dataframe: pd.DataFrame) -> list:
    """
    Returns a list of integers with the complexity grade of the input dataframes transformations
    """
    results = []
    substrings_data = {'Transformation': [], 'Occurrences': []}  # Initialize an empty dictionary to store data

    for string in strings_list:
        total_grade = 0
        for index, row in dataframe.iterrows():
            substring = row['General_Alias']
            grade = row['Complexity_Grade']
            occurrences = string.count(substring)
            total_grade += occurrences * grade
            
            # Append to the substrings_data dictionary if occurrences are not 0
            if occurrences != 0:
                substrings_data['Transformation'].append(substring)
                substrings_data['Occurrences'].append(occurrences)
        
        results.append(total_grade)
    
    # Create a DataFrame from the substrings_data dictionary
    substrings_df = pd.DataFrame(substrings_data)
    
    return results, substrings_df
 



# --------------- block code to create the files ready for sankey diagram data sources -> calcviews ------------------



functions_score = pd.read_excel("data/functions_score.xlsx")[["General_Alias", "Complexity_Grade"]]
functions_score = functions_score.drop_duplicates()
DIR = "data/output-tables/lineages"
save_DIR = "report/data"   
list_files = os.listdir(DIR)
try:
    list_files.remove('lineage-merged.csv')
except:
    pass

df_labels = pd.read_csv("data/output-tables/nodes.csv", sep = ',')
#df = pd.read_csv(f"{DIR}/{list_files[0]}", sep = ',')
df_labels = df_labels.dropna(subset=['FUNCTION'])
Sources = pd.DataFrame()

# Iterate over rows of the DataFrame
for index, row in df_labels.iterrows():
    # Check if '@' is not in the 'LABEL_NODE' column of the current row
    if 'DataSources' in row['FUNCTION']:
        # Extract only the desired columns and rename 'FILTER' to 'COUNT'
        filtered_row = row[['LABEL_NODE', 'ID', 'FILTER']].rename({'FILTER': 'COUNT'})
        
        # Append the filtered row to the empty DataFrame
        Sources = pd.concat([Sources, filtered_row.to_frame().transpose()], ignore_index=True)
Sources['COUNT'] = 0
info_calc = {}

#this takes into account all the xlsx in the folder which are all the tech lineages from all the calc views
for files in list_files:
    df = pd.read_csv(f"{DIR}/{files}", sep = ',')
    source_ids = set(Sources['ID'])
    
    # Iterate over rows in df['SOURCE_NODE']
    for source_id in df['SOURCE_NODE']:
        # Check if the source_id exists in source_ids and has not been counted before
        if source_id in source_ids:
            # Find the index of the ID in Sources
            idx = Sources.index[Sources['ID'] == source_id]
            # Increment the count for the ID by one
            Sources.loc[idx, 'COUNT'] += 1
            # Remove the ID from source_ids to ensure it's only counted once
            source_ids.remove(source_id)


    
    # List of unique nodes
    nodes = list(set(df['TARGET_NODE']) | set(df['SOURCE_NODE']))
    
    # Filter label_nodes and function_nodes based on matching IDs
    label_nodes = df_labels[df_labels['ID'].isin(nodes)][['ID', 'LABEL_NODE']].rename(columns={'LABEL_NODE': 'LABEL_NODE'})
    function_nodes = df_labels[df_labels['ID'].isin(nodes)][['ID', 'FUNCTION']].rename(columns={'FUNCTION': 'FUNCTION'})
    
    # Count occurrences of each node in TARGET_NODE and SOURCE_NODE columns
    target_nodes = df['TARGET_NODE'].value_counts().reset_index().rename(columns={'TARGET_NODE': 'ID', 'count': 'TARGET_COUNT'})
    source_nodes = df['SOURCE_NODE'].value_counts().reset_index().rename(columns={'SOURCE_NODE': 'ID', 'count': 'SOURCE_COUNT'})
    
    # Merge label_nodes and function_nodes on 'ID'
    label_function_nodes = pd.merge(label_nodes, function_nodes, on='ID', how='outer')
    
    # Merge label_function_nodes, target_nodes, and source_nodes on 'ID'
    result = pd.merge(label_function_nodes, target_nodes, left_on='ID', right_on='ID', how='outer')
    result = pd.merge(result, source_nodes, left_on='ID', right_on='ID', how='outer')
    result['TARGET_COUNT'] = result['TARGET_COUNT'].fillna(0)
    result['SOURCE_COUNT'] = result['SOURCE_COUNT'].fillna(0)
    
    info_calc[files.split('-')[1].split('.')[0]] = result
    """ this first part is to calcualtion how much a node is used as a source or a target node based on the columns which are fed into or arise from the node
    """ 
calc_views = list(info_calc.keys())

filtered_data = []
for key, df in info_calc.items():
    """ part to get the sources which source feed data into the calc view. This could also be other caluculation views"""
    filtered_df = df[df['FUNCTION'] == 'DataSources']
    if not filtered_df.empty:
        label_nodes = filtered_df['LABEL_NODE'].tolist()
        filtered_data.append({'CALC_VIEW': key, 'SOURCE': label_nodes})




"""this part is to make the csv files which are used for the sankey where sources are coupled to the calc views"""
# Create a new dataframe from the filtered data
result_df = pd.DataFrame(filtered_data)
result_df = result_df.explode('SOURCE').reset_index(drop=True)
Nodes_source = list(np.unique(result_df['CALC_VIEW']))
Nodes_source.extend(np.unique(result_df['SOURCE']))
Nodes_source = pd.DataFrame(Nodes_source, columns=['Name'])
result_df['CALC_ID'],result_df['SOURCE_ID'],result_df['LINK_VALUE'],result_df['COLOR'] = 0,0,1,'aliceblue'

for i in range(len(result_df)):
    for j in range(len(Nodes_source)):
        if result_df.at[i, 'CALC_VIEW'] == Nodes_source.at[j, 'Name']:
            result_df.at[i, 'CALC_ID'] = j
        elif result_df.at[i, 'SOURCE'] == Nodes_source.at[j, 'Name']:
            result_df.at[i, 'SOURCE_ID'] = j
result_df.to_csv("data/output-tables/analysis/lineage_calc_source.csv", index = False)
Nodes_source.to_csv("data/output-tables/analysis/nodes_calc_source.csv", index = True)


""" these table names correspond with the table names in the report creation file"""
trans_data = pd.DataFrame(columns=['Transformation', 'Occurrences'])

table11 = pd.DataFrame(columns=['Calculation view','Number of nodes', 'Number of transformations', 'Number of filters'])



# --------------- block code to create technical lineages for a calculation view ------------------

table212 = pd.DataFrame(columns=['Calculation view','Node', 'Transformation', 'Complexity Score'])
table2122 = pd.DataFrame(columns=['Calculation view', 'Transformation count', 'Summation complexity Score'])
table211 = pd.DataFrame(columns=['Calculation view','Node', 'Transformation', 'Complexity Score'])

for files in list_files:
    df = pd.read_csv(f"{DIR}/{files}", sep = ',')
    #df = pd.read_csv(f"{DIR}/{list_files[8]}", sep = ',')
    nodes = list(set(df['TARGET_NODE']) | set(df['SOURCE_NODE']))
    
    # Filter label_nodes and function_nodes based on matching IDs
    label_nodes = df_labels[df_labels['ID'].isin(nodes)][['ID', 'LABEL_NODE', 'FUNCTION', 'JOIN_ARGU','FILTER']].rename(columns={'LABEL_NODE': 'LABEL_NODE', 'FUNCTION': 'FUNCTION', 'JOIN_ARGU': 'JOIN_ARGU', 'FILTER': 'FILTER'})
    label_nodes = label_nodes.reset_index(drop=True)
    
    Data = df[['SOURCE_NODE','SOURCE_FIELD','TARGET_NODE','TARGET_FIELD','TRANSFORMATION']].copy()
    sub_join = list(label_nodes['JOIN_ARGU'])
    
    sub_join = [ast.literal_eval(item) if isinstance(item, str) else item for item in sub_join]
    # Iterate through the list and add 'LABEL_NODE' to dictionaries
    for i in range(len(sub_join)):
        if isinstance(sub_join[i], dict):
            # Extract the corresponding 'LABEL_NODE' from label_nodes DataFrame based on its index
            label_node = label_nodes.iloc[i]['LABEL_NODE']
            sub_join[i]['LABEL_NODE'] = label_node
    sub_join = [item for item in sub_join if isinstance(item, dict)]
    list_filters = []
    for filter_value, label_node in zip(label_nodes['FILTER'], label_nodes['LABEL_NODE']):
        if isinstance(filter_value, str):
            list_filters.append({'filter': filter_value, 'LABEL_NODE': label_node, 'Field' : filter_value.split('"')[1]})
    
    
    Data["JOIN_ARGU"] = np.nan
    Data["FILTER"] = np.nan
    for i in range(len(Data)):
        for j in range(len(label_nodes)):
            if Data.at[i, 'SOURCE_NODE'] == label_nodes.at[j, 'ID']:
                Data.at[i, 'SOURCE_NODE'] = label_nodes.at[j, 'LABEL_NODE']
            if Data.at[i, 'TARGET_NODE'] == label_nodes.at[j, 'ID']:
                Data.at[i, 'TARGET_NODE'] = label_nodes.at[j, 'LABEL_NODE']
        for k in range(len(sub_join)):
            if 'LABEL_NODE' in sub_join[k] and sub_join[k]['LABEL_NODE'] == Data.at[i, 'TARGET_NODE'] and sub_join[k].get('JoinVariable') == Data.at[i, 'TARGET_FIELD']:
                updated_dict = sub_join[k].copy()  # Make a copy of the dictionary
                updated_dict.pop('LABEL_NODE', None)  # Remove 'LABEL_NODE' key
                Data.at[i, 'JOIN_ARGU'] = str(updated_dict)
        for l in range(len(list_filters)):
            if list_filters[l]['LABEL_NODE'] == Data.at[i, 'TARGET_NODE'] and list_filters[l].get('Field') == Data.at[i, 'TARGET_FIELD']: 
                Data.at[i, 'FILTER'] = list_filters[l].get('filter')
    
    strings_list = list(Data["TRANSFORMATION"])
    strings_list = [str(x) for x in strings_list]
    grades_list, substrings_df = calculate_grade(strings_list, functions_score)
    substrings_df = substrings_df.groupby('Transformation', as_index=False).sum().reset_index(drop=True)
    Data["Complexity_Score"] = grades_list
    #Data = Data.dropna(subset=['JOIN_ARGU', 'FILTER', 'TRANSFORMATION'], how='all')
    trans_data = pd.concat([trans_data, substrings_df])


    #------------------- block for aggregation ---------------------------
    
    unique_trans = Data['TRANSFORMATION'].dropna().nunique()
    unique_filter = Data['FILTER'].dropna().nunique()
    function_counts = label_nodes["FUNCTION"].value_counts()
    Data_final_tech = Data.dropna(subset=['JOIN_ARGU', 'FILTER', 'TRANSFORMATION'], how='all')
    
    
    Data_final = Data.dropna(subset=['TRANSFORMATION'], how='all')
    
    # Drop duplicate rows based on selected columns
    filtered_df = Data_final.drop_duplicates(subset=['SOURCE_NODE', 'TRANSFORMATION']).reset_index(drop=True)
    for index, row in filtered_df.iterrows():
        if row["SOURCE_FIELD"] in row["SOURCE_NODE"]:
            filtered_df.drop(index=index, inplace=True)
    filtered_df = filtered_df.reset_index(drop=True)   
    filtered_df = filtered_df[['SOURCE_NODE', 'TRANSFORMATION', 'Complexity_Score']]
    filtered_df = filtered_df.rename(columns={'SOURCE_NODE': "Node", 'TRANSFORMATION' : 'Transformation', 'Complexity_Score' : 'Complexity Score'})
    filtered_df['Calculation view'] = files.split('-')[1].split('.')[0]
    filtered_df = filtered_df.reindex(columns=['Calculation view', 'Node', 'Transformation', 'Complexity Score'])
    
    table212 = pd.concat([table212, filtered_df])
    filtered_df = filtered_df.sort_values(by='Complexity Score', ascending=False).head(1)
    table211 = pd.concat([table211, filtered_df])
    
    temp = {'Calculation view': files.split('-')[1].split('.')[0],'Number of nodes' : sum(function_counts), 'Number of transformations' : unique_trans, 'Number of filters' : unique_filter}
    table11.loc[len(table11)] = temp
    calc_scores =  Data[Data['TRANSFORMATION'].notna()].drop_duplicates(subset='TRANSFORMATION').reset_index(drop=True)
    temp = {'Calculation view' : files.split('-')[1].split('.')[0], 'Transformation count' : unique_trans, 'Summation complexity Score' : sum(calc_scores["Complexity_Score"])}
    table2122.loc[len(table2122)] = temp
    if files == 'lineage-Q_AccountsPayable.csv':
        substrings_df.to_csv(f"{save_DIR}/substrings_df.csv",index = False)
        Data_final_tech.to_csv(f"{save_DIR}/Data_final_tech .csv",index = False)
        Account_payable_tech_lineage = Data
table212 = table212.sort_values(by='Complexity Score', ascending=False).head(5).reset_index(drop=True)  
table2122 = table2122.sort_values(by='Summation complexity Score', ascending=False).head(5).reset_index(drop=True)  

trans_data = trans_data.groupby('Transformation', as_index=False).sum().reset_index(drop=True)   
table112 = Sources.sort_values(by='COUNT',ascending=False) 
table112 = table112.drop(columns=['ID'], axis=1).head(5).reset_index(drop=True)
#print(sorted_sources)
calc_names = []
table22 = pd.DataFrame(columns=['Calculation view','Input calculation view'])
table31 = pd.DataFrame(columns=['Calculation view','Data source','Columns used','Columns in source','Percentage columns used'])
columns_tables = pd.read_excel("data/Columns_sources.xlsx").dropna().reset_index(drop=True)
for files in list_files:
    calc_names.append(files.split('-')[1].split('.')[0])
for i in info_calc.keys():
    for j in range(len(info_calc[i])):
        if info_calc[i]['LABEL_NODE'][j] in calc_names:
            temp = {'Calculation view': i,'Input calculation view' : info_calc[i]['LABEL_NODE'][j]}
            table22.loc[len(table22)] = temp
        for k in range(len(columns_tables)):  
            if info_calc[i]['LABEL_NODE'][j] == columns_tables['LABEL_NODE'][k] and info_calc[i]['FUNCTION'][j] == "DataSources":
                temp = {'Calculation view': i,'Data source' : info_calc[i]['LABEL_NODE'][j], 'Columns used' : info_calc[i]['SOURCE_COUNT'][j], 'Columns in source' :  columns_tables['COUNT'][k], "Percentage columns used" : info_calc[i]['SOURCE_COUNT'][j]/columns_tables['COUNT'][k]}
                table31.loc[len(table31)] = temp
    
table113 = trans_data.sort_values(by='Occurrences', ascending=False).head(5).reset_index(drop=True)



table11.to_csv(f"{save_DIR}/table11.csv",index = False)
table112.to_csv(f"{save_DIR}/table112.csv",index = False)
table113.to_csv(f"{save_DIR}/table113.csv",index = False)
table2122.to_csv(f"{save_DIR}/table2122.csv",index = False)
table211.to_csv(f"{save_DIR}/table211.csv",index = False)
table212.to_csv(f"{save_DIR}/table212.csv",index = False)
table22.to_csv(f"{save_DIR}/table22.csv",index = False)
table31.to_csv(f"{save_DIR}/table31.csv",index = False)























