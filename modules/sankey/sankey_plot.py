from flask import Flask, render_template, request
import plotly.graph_objs as go
import pandas as pd

def draw_sankey(name, path):
    """
    Dashboard logic
    """
    if len(name) == 1:
        df = pd.read_csv(f'{path}/lineage-{name[0]}.csv', sep = ',')
        title = f"Sankey of calculationview: {name[0]}"
    else:
        lineage_list = []
        if len(name) == 2:
            title = f"Sankey of merged calculationviews: {name[0]} and {name[1]}"
        else:
            title = "Sankey of multiple merged calculationviews"
        for lins in name: 
            lineage = pd.read_csv(f"{path}/lineage-{lins}.csv")
            lineage_list.append(lineage)
        df = pd.concat(lineage_list, ignore_index=True)



    df['source_to_target'] = df[['SOURCE_FIELD', 'TARGET_FIELD']].agg('=>'.join, axis=1)
    df['source_to_target_transformation'] = df[['source_to_target','TRANSFORMATION']].apply(lambda x : '{}<br />Transformation: {}'.format(x[0],x[1]), axis=1)

    df_labels = pd.read_csv('data/output-tables/nodes.csv')
    df_labels['hover_label'] = df_labels[['LABEL_NODE','WHERE']].apply(lambda x : '{}<br />Filter: {}'.format(x[0],x[1]), axis=1)

    fig = go.Figure(data=[go.Sankey(
        node = dict(
            pad = 20,
            thickness = 20,
            line = dict(color = "black", width = 0.5),
            label = df_labels['LABEL_NODE'],
            customdata = df_labels['hover_label'],
            hovertemplate='%{customdata}',
            color = df_labels['COLOR']
        ),
        link = dict(
        #arrowlen=15,
        line = dict(color = "blue", width = 0.05),
        hoverlabel = dict (font = dict(size=15) ),
        source = df['SOURCE_NODE'], # indices correspond to labels, eg A1, A2, A2, B1, ...
        target = df['TARGET_NODE'],
        value = df['LINK_VALUE'],
        customdata = df['source_to_target_transformation'],
        hovertemplate='Details: %{customdata}',
        color = df['COLOR'],
    ),
    
        
    )])
    fig.update_layout(font_size=10)
    fig.update_layout(title = dict(text=title, font_size=20))
    return fig