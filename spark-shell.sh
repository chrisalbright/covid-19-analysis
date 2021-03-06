docker run --rm -it \
-p 4040:4040 \
--network covid-19-analysis_elastic-net \
-v $(pwd)/COVID-19:/data \
-v $(pwd)/target:/spark \
gettyimages/spark \
bin/spark-shell \
--jars /spark/covid-19-analysis-0.1-SNAPSHOT.jar \
--packages org.elasticsearch:elasticsearch-hadoop:7.6.1