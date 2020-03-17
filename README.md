# Dashboard for COVID-19

Data sourced from [Johns Hopkins](https://github.com/CSSEGISandData/COVID-19)

Requires Docker && Docker Compose - You'll probably need to increase the resources available to Docker

How to use:

1. `docker-compose up`
This will start ElasticSearch & Kibana
1. `./import-kibana-objects.sh`
This will import all the visualizations and dashboards
1. `./run-import.sh`
This will process the data and save it into an ElasticSearch index
1. `open http://localhost:5601/`
This is the URL for the your Kibana instance

This project is very new. Please lmk if you use it, have trouble with it, have success with it, or improve it.

Thanks!