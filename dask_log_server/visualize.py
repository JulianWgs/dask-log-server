import random
import pandas as pd
import dask

from . helper import _to_tuple


def visualize(dsk, df_tasks, label="", color="", current_time=0, **kwargs):
    """
    Draw a dask graph enhanced by additional information.

    Parameters
    ----------
    dsk: dict
        Dask task graph. Should be able to be plotted by dask.visualize.
    df_tasks: pd.DataFrame
        DataFrame of the dask task stream data. "key" column is mandatory to
        assign a row of the DataFrame to a node in the graph. "key" column
        must be of type string even when key is a tuple, because otherwise
        the type is not compatible with formats like parquet.
    label: str
        Column name of df_tasks DataFrame which contains the value for the
        node label.
    color: str
        Column name of df_tasks DataFrame which contains color information of
        node fill color.

        If the values are numerical the node is filled with grayscale tones.
        The label font color is adjusted to be always readable.

        If the values are strings each unique value is assigned a different
        color.

        If the value is "progress" each started node is filled with red and
        each finished is filled with blue. To set the current time use the
        argument "current_time". The option needs the columns "start_delta"
        and "stop_delta" in the df_tasks DataFrame containing the seconds
        passed since the start of the graph execution.
    current_time: float
        If color is set to "progress" this sets the current time influencing
        the fill color of the nodes.
    """
    attributes = _get_dsk_attributes(
        dsk, df_tasks, label_col=label, color_col=color, current_time=current_time
    )

    return dask.visualize(
        dsk,
        data_attributes=attributes["data"],
        function_attributes=attributes["func"],
        **kwargs,
    )


def _get_dsk_attributes(dsk, df_tasks, label_col="", color_col="", current_time=0.0):
    """
    See visualize for doc string.
    """
    # Filter unnecessary tasks, dtype of key is str
    df_tasks = df_tasks[df_tasks["key"].isin([str(key) for key in dsk.keys()])]
    if color_col == "progress" or color_col == "":
        color_type = "progress"
    elif pd.api.types.is_numeric_dtype(df_tasks[color_col]):
        color_type = "float"
        max_color_value = df_tasks[color_col].max()
    elif pd.api.types.is_string_dtype(
            df_tasks[color_col]
    ) or pd.api.types.is_categorical_dtype(df_tasks[color_col]):
        color_type = "category"
        random.seed(10)
        unique_colors = {
            value: f"#{random.randint(0, 0xFFFFFF):06X}"
            for value in df_tasks[color_col].dropna().unique()
        }
    else:
        raise ValueError("Could not get type on how to color graph.")

    attributes = {"func": dict(), "data": dict()}
    for index, df_single_task in df_tasks.iterrows():
        if df_single_task["action"] == "compute":
            attribute_name = "func"
        else:
            attribute_name = "data"
        key = df_single_task["key"]
        attributes[attribute_name][key] = {}
        if label_col != "":
            label = df_single_task[label_col]
            if not pd.isna(label):
                attributes[attribute_name][key]["label"] = str(label)
        if color_col == "progress":
            if df_single_task["stop_delta"] < current_time:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = "blue"
            elif df_single_task["start_delta"] < current_time:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = "red"
        elif color_type == "float":
            try:
                # Minimum value is 0
                grayscale = 100 - int(df_single_task[color_col] / max_color_value * 100)
            except ValueError:
                continue
            attributes[attribute_name][key]["style"] = "filled"
            attributes[attribute_name][key]["fillcolor"] = f"gray{grayscale}"
            if grayscale < 20:
                attributes[attribute_name][key]["fontcolor"] = "white"
        elif color_type == "category":
            color = unique_colors.get(df_single_task[color_col])
            if color is not None:
                attributes[attribute_name][key]["style"] = "filled"
                attributes[attribute_name][key]["fillcolor"] = color

    attributes["data"] = {
        _to_tuple(key): value for key, value in attributes["data"].items() if value
    }
    attributes["func"] = {
        _to_tuple(key): value for key, value in attributes["func"].items() if value
    }
    return attributes
