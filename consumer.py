from confluent_kafka import KafkaError
from confluent_kafka import TopicPartition
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

tp = TopicPartition('pure_project_xml', 0, 0)
c = AvroConsumer({
  'bootstrap.servers': 'localhost:9092',
  'group.id': 'pure_project_output_generator',
  'schema.registry.url': 'http://localhost:8081',
})
c.assign([tp])
assignment = c.assignment()

# Need a timeout here due to this bug: https://github.com/confluentinc/confluent-kafka-python/issues/196
(first_offset, next_offset_to_create) = c.get_watermark_offsets(tp, timeout=1, cached=False)
last_offset = next_offset_to_create - 1

f = open('pure_project.xml', 'w')
f.write(
  '<?xml version="1.0"?>' + "\n" +
  '<project:upmprojects xmlns:common="v3.commons.pure.atira.dk" xmlns:project="v1.upmproject.pure.atira.dk">' + "\n"
)

# range values explained: We read the topic backwards, starting with the 
# last offset. We use `first_offset - 1` because Python's range will stop
# before it reaches that value. So the last offset used will actually be
# the first offset. The last argument is the step, for which we pass -1,
# because we're reading backwards.
for offset in range(last_offset, first_offset - 1, -1):

  # Since Kafka Consumers normally read messages fro oldest to newest, we
  # manually set the offset to read:
  # TODO: Can we ensure that this offset actually exists somehow?
  tp.offset = offset
  c.assign([tp])

  msg = c.poll(10)
  value = msg.value()
  f.write(value['xml'] + "\n")

c.close()

f.write('</project:upmprojects>' + "\n")
