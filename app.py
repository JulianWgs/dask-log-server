import os
import base64
import pickle
import optparse

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import dash_table

import pandas as pd
import distributed
import dask
import dask.dot

import dask_log_server

parser = optparse.OptionParser()
options, args = parser.parse_args()
log_path = args[0]

external_stylesheets = ["https://codepen.io/chriddyp/pen/bWLwgP.css"]

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

df = pd.read_parquet(os.path.join(log_path, "graphs.parquet"))
df_tasks = pd.read_parquet(os.path.join(log_path, "tasks.parquet"))

options = [
    {"label": item, "value": item}
    for item in ["worker", "duration", "nbytes", "typename"]
]

drop_cols = ["status", "thread", "type", "datetime", "client_id", "id"]
task_columns = [
    {"name": item, "id": item}
    for item in [
        "action",
        "start",
        "stop",
        "worker",
        "status",
        "nbytes",
        "thread",
        "type",
        "typename",
        "key",
        "datetime",
        "client_id",
        "id",
        "duration",
    ]
    if item not in drop_cols
]

graph_cols = df.columns

graph_col_options = [{"label": item, "value": item} for item in graph_cols]

app.layout = html.Div(
    [
        html.Div(
          dcc.Dropdown("graph-columns", options=graph_col_options, multi=True, value=['start', 'stop', 'duration', 'n_workers', 'n_tasks', 'client_id'])
        ),
        html.Div(
            [
                dash_table.DataTable(
                    id="graph-table",
                    columns=[{"name": i, "id": i} for i in df.columns],
                    data=df.to_dict("records"),
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
                ),
            ]
        ),
        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            ["Node Color", dcc.Dropdown(id="color", options=options,)]
                        ),
                    ],
                    className="three columns",
                ),
                html.Div(
                    [
                        html.Label(
                            ["Node Label", dcc.Dropdown(id="label", options=options,)]
                        ),
                    ],
                    className="three columns",
                ),
            ],
            className="row",
        ),
        html.Div([html.Img(id="graph", height=300),], className="row"),
        html.Div(
            [
                dash_table.DataTable(
                    id="tasks-table",
                    columns=task_columns,
                    editable=True,
                    filter_action="native",
                    sort_action="native",
                    sort_mode="multi",
                    column_selectable=False,
                    row_selectable=False,
                    row_deletable=False,
                    selected_columns=[],
                    selected_rows=[],
                    page_action="native",
                    page_current=0,
                    page_size=10,
                )
            ],
            className="row",
        ),
    ]
)


@app.callback(
    [
        Output(component_id="graph-table", component_property="columns"),
        Output(component_id="graph-table", component_property="data"),
    ],
    [
        Input(component_id="graph-columns", component_property="value"),
    ],
)
def update_graph_table(column_names):
    return [{"name": i, "id": i} for i in column_names], df[column_names].to_dict("records")


@app.callback(
    Output(component_id="graph", component_property="src"),
    [
        Input(component_id="graph-table", component_property="selected_rows"),
        Input(component_id="color", component_property="value"),
        Input(component_id="label", component_property="value"),
    ],
)
def load_graph(selected_rows, color, label):
    if not selected_rows:
        return ""
    if color is None:
        color = ""
    if label is None:
        label = ""
    graph_id = df.iloc[selected_rows].index[0]
    filename = os.path.join(log_path, "graph_" + graph_id + ".dsk")
    with open(filename, "rb") as file:
        dsk = distributed.protocol.deserialize(*pickle.load(file))
    attributes = dask_log_server._get_dsk_attributes(
        dsk, df_tasks, label=label, color=color,
    )
    try:
        data = dask.dot.to_graphviz(
            dsk, data_attributes=attributes["data"], function_attributes=attributes["func"]
        ).pipe(format="png")
    except RuntimeError:
        data = b""
    encoded_image = base64.b64encode(data).decode()
    return "data:image/png;base64,{}".format(encoded_image)


@app.callback(
    Output(component_id="tasks-table", component_property="data"),
    [Input(component_id="graph-table", component_property="selected_rows")],
)
def tasks_of_graph(selected_rows):
    if not selected_rows:
        return list()
    graph_id = df.iloc[selected_rows].index[0]
    return (
        df_tasks[df_tasks["id"] == graph_id].drop(columns=drop_cols).to_dict("records")
    )


if __name__ == "__main__":
    app.run_server(debug=True)
