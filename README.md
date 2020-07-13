# Dask Log Server

> The goal of this project is to preserve all necessary runtime data of a Dask client in order to "replay" and analyze the performance and behavior of the client after the fact.

Here you can see an early prototype of the dashboard. The top table shows all executed task graphs. The selected task graph is visualized. The table at the bottom contains information of each task, which was executed during that graph execution. With the two drop down menus it is possible to enhance the visualization of the graph with additional information. Right now the color is set to the duration of the task and the label is set to the size of the data. Other options are worker and typename for both color and label, but every numerical or string value can be used (including custom ones). All the information in the dashboard can also be accessed programmatically.

![Early Prototype](images/early_protype.png)

Below is the vision of the project, so you know which direction it will take. Feel free to leave feedback.

## Client

The goal is to collect the logs via a plugin of the client class of dask distributed. With HTML output in Jupyter Lab you can start and stop logging and get info about the current status of the logger.

## Server Collection

I'm currently not sure what to use as a logging server. For now I'll probably just implement a RestAPI and save it to S3. The collection server should be able to collect logs from multiple sources.

## Server Dashboard

The goal is to display the same graphics which are currently available in the dask dashboard, but for all collected data from the past. Additionally new visualization may be needed which compare task graphs or aggregate the information of multiple task graphs.

For now this is only a side project, but it would be really cool to make this an official Dask repository.

