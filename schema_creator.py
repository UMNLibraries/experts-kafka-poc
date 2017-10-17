from confluent_kafka import avro
import json
import requests

# NOTE: (Confluent's?) Avro does NOT like dashes in name values!
# The following causes this error: {"error_code":42201,"message":"Input schema is an invalid Avro schema"}%
#topic = 'pure-project-xml'

topic = 'pure_project_xml'
dict_schema = {
  'type': 'record',
  'name': topic,
  'fields': [
      {'name': 'project_id', 'type': 'string'},
      {'name': 'xml', 'type': 'string'},
  ],
}

json_schema = json.dumps(dict_schema)

f = open(topic + '.avsc', 'w')
f.write(json_schema)

schema_payload = { 'schema': json_schema }

url = 'http://localhost:8081/subjects/' + topic + '/versions'
headers = {'Content-Type': 'application/vnd.schemaregistry.v1+json'}
r = requests.post(url, json=schema_payload, headers=headers)

print(r.status_code)
print(r.json())
