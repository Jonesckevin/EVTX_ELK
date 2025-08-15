#!/bin/sh

echo "Waiting for Kibana to be fully ready..."
sleep 30

echo "Importing saved objects..."

for file in /saved_objects/*.ndjson; do
  if [ -f "$file" ]; then
    echo "Importing $file"
    
    # Use proper curl syntax for file upload
    curl -X POST "http://kibana:5601/api/saved_objects/_import" \
         -H "kbn-xsrf: true" \
         -F "file=@$file"
    
    if [ $? -eq 0 ]; then
      echo "Successfully imported $file"
    else
      echo "Failed to import $file"
    fi
  fi
done

echo "Saved objects import complete!"
