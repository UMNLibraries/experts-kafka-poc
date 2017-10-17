from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer

value_schema = avro.load('pure_project_xml.avsc')
print(value_schema)

p = AvroProducer(
  {
    'bootstrap.servers': 'localhost:9092',
    'schema.registry.url': 'http://localhost:8081',
  },
  default_value_schema=value_schema
)

projects = [
  {
    'project_id': 'CON000000000001',
    'xml': '<project:upmproject id="CON000000000001" type="research"><project:title><common:text lang="en" country="US">Pyrolysis Pilot Project</common:text></project:title><project:descriptions><common:description type="projectdescription"><common:text lang="en" country="US">Pyrolysis Pilot Project</common:text></common:description></project:descriptions><project:ids><common:id type="sponsor_award_number">Chapter 30, Subd. 5o</common:id><common:id type="um_award_number">CON000000000001</common:id></project:ids><project:internalParticipants><project:internalParticipant><project:personId>2396</project:personId><project:organisationIds><project:organisation id="BIPTHQRCQ"></project:organisation></project:organisationIds><project:role>pi</project:role></project:internalParticipant></project:internalParticipants><project:externalParticipants></project:externalParticipants><project:managedByOrganisation id="BIPTHQRCQ"></project:managedByOrganisation><project:startDate>2007-07-01</project:startDate><project:endDate>2010-06-30</project:endDate><project:relatedAwards><project:relatedAwardId>04086247</project:relatedAwardId></project:relatedAwards><project:visibility>Public</project:visibility></project:upmproject>',
  },
  {
    'project_id': 'CON000000000002',
    'xml': '<project:upmproject id="CON000000000002" type="research"><project:title><common:text lang="en" country="US">Land Acquisition, Minnesota Landscape Arboretum - Contin</common:text></project:title><project:descriptions><common:description type="projectdescription"><common:text lang="en" country="US">Land Acquisition, Minnesota Landscape Arboretum - Continuation</common:text></common:description></project:descriptions><project:ids><common:id type="sponsor_award_number">Sec (9), subd. (6L)</common:id><common:id type="um_award_number">CON000000000002</common:id></project:ids><project:internalParticipants></project:internalParticipants><project:externalParticipants><project:externalParticipant><project:firstName>Peter</project:firstName><project:lastName>Olin</project:lastName><project:role>pi</project:role></project:externalParticipant></project:externalParticipants><project:managedByOrganisation id="BKNWGPPBO"></project:managedByOrganisation><project:startDate>2003-07-01</project:startDate><project:endDate>2010-06-30</project:endDate><project:relatedAwards><project:relatedAwardId>04199013</project:relatedAwardId></project:relatedAwards><project:visibility>Public</project:visibility></project:upmproject>',
  },
  {
    'project_id': 'CON000000000002',
    'xml': '<project:upmproject id="CON000000000003" type="research"><project:title><common:text lang="en" country="US">2005 Land Acquisition, Minnesota Landscape Arboretum</common:text></project:title><project:descriptions><common:description type="projectdescription"><common:text lang="en" country="US">2005 Land Acquisition, Minnesota Landscape Arboretum</common:text></common:description></project:descriptions><project:ids><common:id type="sponsor_award_number">Sec.11, subd. 06 (p)</common:id><common:id type="um_award_number">CON000000000003</common:id></project:ids><project:internalParticipants></project:internalParticipants><project:externalParticipants><project:externalParticipant><project:firstName>Peter</project:firstName><project:lastName>Olin</project:lastName><project:role>pi</project:role></project:externalParticipant></project:externalParticipants><project:managedByOrganisation id="BKNWGPPBO"></project:managedByOrganisation><project:startDate>2005-07-01</project:startDate><project:endDate>2010-06-30</project:endDate><project:relatedAwards><project:relatedAwardId>04199015</project:relatedAwardId></project:relatedAwards><project:visibility>Public</project:visibility></project:upmproject>',
  },
]

for project in projects:
  p.produce(
    topic='pure_project_xml',
    value=project
  )
  p.flush()
