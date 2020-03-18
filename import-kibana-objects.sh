curl -X POST \
-H "kbn-xsrf: true" \
--form file=@kibana-objects.ndjson \
http://localhost:5601/api/saved_objects/_import?overwrite=true
