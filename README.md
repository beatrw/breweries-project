# Breweries Project

A project in an **airflow+docker** environment that consumes data from the
Open Brewery DB API, transforming it and persisting it in a **Google Cloud Storage** data lake,
following the medallion architecture with three layers: raw data,
selected data partitioned by location and an aggregated analytical layer.


In the documentation folder are all the instructions on how to set up the docker
environment, as well as separate sessions for each module of the project,
detailing the architecture, choices made and ideas for monitoring and alerts.

🍺