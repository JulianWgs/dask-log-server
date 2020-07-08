import os
import base64
import pickle
import optparse
import pandas as pd
import dask
import dask.dot
import distributed

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table

parser = optparse.OptionParser()
options, args = parser.parse_args()
log_path = args[0]

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

df = pd.read_parquet(os.path.join(log_path, "graph.parquet"))

app.layout = html.Div([
    html.Div([
        html.Img(
            id="graph",
            height=300)
    ]),
    dash_table.DataTable(
        id='table',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        editable=True,
        filter_action="native",
        sort_action="native",
        sort_mode="multi",
        column_selectable=False,
        row_selectable="single",
        row_deletable=False,
        selected_columns=[],
        selected_rows=[],
        page_action="native",
        page_current=0,
        page_size=10,
    )

])


@app.callback(
    Output(component_id='graph', component_property='src'),
    [Input(component_id='table', component_property='selected_rows')]
)
def load_graph(selected_rows):
    if not selected_rows:
        return ""
    graph_id = df.iloc[selected_rows].index[0]
    filename = os.path.join(log_path, "graph_" + graph_id + ".dsk")
    with open(filename, "rb") as file:
        dsk = distributed.protocol.deserialize(*pickle.load(file))
    data = dask.dot.to_graphviz(dsk).pipe(format="png")
    encoded_image = base64.b64encode(data).decode()
    return 'data:image/png;base64,{}'.format(encoded_image)


if __name__ == '__main__':
    app.run_server(debug=True)
