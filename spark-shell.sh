docker run --rm -it \
-p 4040:4040 \
--network covid-19-analysis_elastic-net \
-v $(pwd)/COVID-19:/data \
-v $(pwd)/spark:/spark \
gettyimages/spark \
bin/spark-shell \
--packages org.elasticsearch:elasticsearch-hadoop:7.6.1