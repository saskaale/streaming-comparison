from pyflink.common import Row
# from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.serialization import SimpleStringSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.table.window import Slide
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


def datastream_api_demo():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues

    # print("file:///app/src/kafka-clients-2.8.0.jar")

    env.add_jars("file:///app/src/kafka-clients-2.8.0.jar")
    env.add_jars("file:///app/src/flink-connector-kafka_2.12-1.12.3.jar")

    # 2. create source DataStream
    # deserialization_schema = JsonRowDeserializationSchema.builder() \
    #     .type_info(type_info=Types.ROW([Types.STRING()])).build()
        # .type_info(type_info=Types.STRING()).build()

    kafka_source = FlinkKafkaConsumer(
        topics='quickstart-events',
        deserialization_schema=SimpleStringSchema(),
        properties={'bootstrap.servers': '40.89.150.165:9092', 'group.id': 'test_group'})

    ds = env.add_source(kafka_source)
    # ds.print()

        # env.add_java_source(consumer) \
        # .output()

    # # 3. define the execution logic
    ds = ds.windowAll(SlidingProcessingTimeWindows(Time.seconds(10), Time.seconds(10)))

    # ds = ds.map(lambda a: Row(a, 1), output_type=Types.ROW([Types.STRING(), Types.LONG()])) \
    #        .key_by(lambda a: a[0]) \
    #        .reduce(lambda a, b: Row(a[0], a[1] + b[1]))

    # 4. create sink and emit result to sink
    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.STRING(), Types.LONG()])).build()
    kafka_sink = FlinkKafkaProducer(
        topic='out-topic1',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': '40.89.150.165:9092', 'group.id': 'test_group2'})
    ds.add_sink(kafka_sink)

    # 5. execute the job
    env.execute('datastream_api_demo')


if __name__ == '__main__':
    datastream_api_demo()