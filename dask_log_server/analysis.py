import dask.dataframe as dd


def get_actual_concurrency(df_tasks):
    """
    Calculate worker concurrency of task stream.

    Note: concurrency_sum should have two data points per timestamp with a
    vertical line connecting them. This is not yet implemented.

    Another Note: Melt puts value columns after one another.
    """
    assert isinstance(df_tasks, dd.DataFrame), "Can only use Dask Dataframes"
    df_concurrency = (
        df_tasks[(df_tasks["action"] == "compute")]
        .melt(
            id_vars=["client_id"], value_vars=["start", "stop"], value_name="datetime"
        )
        .set_index("datetime")
    )
    df_concurrency["concurrency"] = df_concurrency["variable"].map(
        {"start": 1, "stop": -1}
    )
    df_concurrency["concurrency_sum"] = df_concurrency.groupby("client_id")[
        "concurrency"
    ].cumsum()
    return df_concurrency


