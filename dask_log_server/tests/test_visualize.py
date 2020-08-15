import datetime

import pandas as pd
import pytest

from dask_log_server.visualize import _get_dsk_attributes


def test_get_dsk_attributes_float_color():
    dsk = {char: None for char in ["A", "B"]}

    df_tasks = pd.DataFrame(
        [
            {"key": "A", "action": "compute", "color": 20},
            {"key": "B", "action": "transfer", "color": 5.5},
        ]
    )

    dsk_attributes = _get_dsk_attributes(dsk, df_tasks, color_col="color")
    dsk_attributes_test = {
        "func": {"A": {"style": "filled", "fillcolor": "gray0", "fontcolor": "white"}},
        "data": {"B": {"style": "filled", "fillcolor": "gray73"}},
    }

    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_progress_color():
    dsk = {char: None for char in ["A", "B"]}

    df_tasks = pd.DataFrame(
        [
            {"key": "A", "action": "compute", "start_delta": 1, "stop_delta": 3},
            {"key": "B", "action": "transfer", "start_delta": 2, "stop_delta": 5},
            {"key": "C", "action": "compute", "start_delta": 6, "stop_delta": 7},
        ]
    )

    dsk_attributes = _get_dsk_attributes(
        dsk, df_tasks, color_col="progress", current_time=4,
    )
    dsk_attributes_test = {
        "func": {"A": {"style": "filled", "fillcolor": "blue"}},
        "data": {"B": {"style": "filled", "fillcolor": "red"}},
    }
    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_category_color():
    dsk = {char: None for char in ["A", "B"]}

    df_tasks = pd.DataFrame(
        [
            {"key": "A", "action": "compute", "color": "Z"},
            {"key": "B", "action": "transfer", "color": "Y"},
        ]
    )

    dsk_attributes = _get_dsk_attributes(dsk, df_tasks, color_col="color")
    dsk_attributes_test = {
        "func": {"A": {"style": "filled", "fillcolor": "#10AEFD"}},
        "data": {"B": {"style": "filled", "fillcolor": "#DB9758"}},
    }
    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_labels():
    dsk = {
        "A": None,
    }

    df_tasks = pd.DataFrame([{"key": "A", "action": "compute", "label": "label"},])

    dsk_attributes = _get_dsk_attributes(dsk, df_tasks, label_col="label")
    dsk_attributes_test = {
        "func": {"A": {"label": "label"},},
        "data": {},
    }
    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_na_categorical_color():
    dsk = {char: None for char in ["A", "B", "C"]}

    df_tasks = pd.DataFrame(
        [
            {"key": "A", "action": "compute"},
            {"key": "B", "action": "compute", "color": "color"},
            {"key": "C", "action": "compute"},
        ]
    )

    dsk_attributes = _get_dsk_attributes(dsk, df_tasks, color_col="color",)
    dsk_attributes_test = {
        "func": {"B": {"fillcolor": "#10AEFD", "style": "filled"},},
        "data": {},
    }

    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_na_float_color():
    dsk = {char: None for char in ["A", "B", "C"]}

    df_tasks = pd.DataFrame(
        [
            {"key": "A", "action": "compute"},
            {"key": "B", "action": "compute", "color": 2.2},
            {"key": "C", "action": "compute"},
        ]
    )

    dsk_attributes = _get_dsk_attributes(dsk, df_tasks, color_col="color",)
    dsk_attributes_test = {
        "func": {"B": {"fillcolor": "gray0", "fontcolor": "white", "style": "filled"},},
        "data": {},
    }

    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_na_label():
    dsk = {char: None for char in ["A", "B", "C"]}

    df_tasks = pd.DataFrame(
        [
            {"key": "A", "action": "compute"},
            {"key": "B", "action": "compute", "label": "label"},
            {"key": "C", "action": "compute"},
        ]
    )

    dsk_attributes = _get_dsk_attributes(dsk, df_tasks, label_col="label",)
    dsk_attributes_test = {
        "func": {"B": {"label": "label"},},
        "data": {},
    }

    assert dsk_attributes == dsk_attributes_test


def test_get_dsk_attributes_wrong_color_dtype():
    dsk = {char: None for char in ["A"]}

    df_tasks = pd.DataFrame(
        [{"key": "A", "action": "compute", "color": datetime.datetime.today()},]
    )

    with pytest.raises(ValueError):
        _get_dsk_attributes(
            dsk, df_tasks, color_col="color",
        )
