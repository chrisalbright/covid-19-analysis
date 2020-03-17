curl -XPOST \
-H 'Content-Type: application/json' \
-H 'kbn-xsrf: true' \
-d '{"type": ["canvas-element", "canvas-workpad", "config", "dashboard", "index-pattern", "lens", "map", "query", "search", "url", "visualization"]}' \
-o kibana-export.ndjson \
http://localhost:5601/api/saved_objects/_export