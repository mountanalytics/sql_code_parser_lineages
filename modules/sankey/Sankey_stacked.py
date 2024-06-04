import pandas as pd
import plotly.graph_objects as go

def sankey_stacked():
    df = pd.read_csv('C:/Users/ErwinSiegers/Documents/GitHub/sas_code_parser/output-tables/analysis/lineage_calc_source1.csv', sep = ',')
    df_labels = pd.read_excel('C:/Users/ErwinSiegers/Documents/GitHub/sas_code_parser/output-tables/analysis/nodes_calc_source.xlsx')
    df = df.rename(columns = {"CALC_VIEW" : "TARGET_FIELD", 'SOURCE' : 'SOURCE_FIELD', "CALC_ID" : "TARGET_NODE", "SOURCE_ID" : "SOURCE_NODE"})
    df_labels = df_labels.rename(columns = {"Unnamed: 0" : "LABEL_NODE"})
    
    df['source_to_target'] = df[['SOURCE_FIELD', 'TARGET_FIELD']].agg('=>'.join, axis=1)
    
    
    
    fig = go.Figure(data=[go.Sankey(
        node = dict(
          pad = 20,
          thickness = 20,
          line = dict(color = "black", width = 0.5),
          label = df_labels['Name'],
          color = df_labels['Color']
         
          #hovertemplate='Node %{customdata} has total value %{value}<extra></extra>',
          #color = "blue"
    
        ),
        link = dict(
          arrowlen=15,
          line = dict(color = "blue", width = 0.05),
          hoverlabel = dict (font = dict(size=15) ),
          source = df['SOURCE_NODE'], 
          target = df['TARGET_NODE'],
          value = df['LINK_VALUE'],
          customdata = df['source_to_target'],
          hovertemplate='Details: %{customdata}',
          color = df['COLOR'],
    
          #hovertemplate='Link from node %{source.customdata}<br />'+'to node%{target.customdata}<br />has value %{value}'+ '<br />and data %{customdata}<extra></extra>',
      ),
      )])
    
    fig.update_layout(font_size=10)
    fig.update_layout(title = dict(text="Data sources coupled to the calculation views", font_size=20))
    return fig