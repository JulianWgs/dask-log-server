# Dask Log Server

> The goal of this project is to preserve all necessary runtime data of a Dask client in order to "replay" and analyze the performance and behavior of the client after the fact.

Following is the vision of the project, so you know which direction it will take. Feel free to leave feedback.

## Client

The goal is to collect the logs via a plugin of the client class of dask distributed. With html output in Jupyter Lab you can start and stop logging and get a little bit of info about the status or problems.

## Server Collection

I'm currently not sure what to use as a logging server. For now I'll probably just implement a RestAPI and save it to S3. The collection server should be able to collect logs from multiple sources.

## Server Serving

The goal is to display the same graphics which are currently available in the dask dashboard, but for all collected data from the past. Additionally new visualization may be needed which compare or aggregate multiple task graphs.

For now this is only a side project, but it would be really cool to make this an official Dask repository.