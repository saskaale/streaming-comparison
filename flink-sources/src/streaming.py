import os
from pyflink.table.expressions import lit, col, Expression
from pyflink.datastream import StreamExecutionEnvironment, TimeCharacteristic
from pyflink.datastream import CheckpointingMode, ExternalizedCheckpointCleanup
from pyflink.table import StreamTableEnvironment, DataTypes, EnvironmentSettings, CsvTableSink, TableConfig
from pyflink.table.descriptors import Schema, Rowtime, Json, Kafka
from pyflink.table.window import Slide

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///app/src/kafka-clients-2.8.0.jar")
    env.add_jars("file:///app/src/flink-connector-kafka_2.12-1.12.3.jar")
    env.set_parallelism(1)
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    env.enable_checkpointing(60000, CheckpointingMode.EXACTLY_ONCE)
    config = env.get_checkpoint_config()
    config.enable_externalized_checkpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION)

    st_env = StreamTableEnvironment.create(
        env,
        environment_settings=EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
    )

    print("register kafka source")
    register_kafka_source(st_env)
    print("register transaction sinks")
    register_transactions_sink_into_csv(st_env)


    st_env.from_path("source_tbl") \
        .window(Slide.over(lit(1).minute).every(lit(5).seconds).on("ts").alias("w")) \
        .group_by(col("w"), col("message")) \
        .select("""
                    count(message) as total,
                    message,
                    w.end as end_time
                   """) \
        .insert_into("topk_sink")


    st_env.execute("app")

def register_kafka_source(st_env):

    sink_ddl = f"""
        CREATE TABLE source_tbl (
            message STRING,
            `ts` TIMESTAMP METADATA FROM 'timestamp',
            proctime AS PROCTIME(), -- use computed column to define proctime attribute
            WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- use WATERMARK statement to define rowtime attribute
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'quickstart-events-jsonobj2',
            'scan.startup.mode' = 'earliest-offset',
            'properties.bootstrap.servers' = '40.89.150.165:9092',
            'format' = 'json'
            )
        """
    st_env.execute_sql(sink_ddl)

    return



def register_transactions_sink_into_csv(st_env):

    sink_ddl = f"""
        CREATE TABLE total_sink (
            total BIGINT,
            end_time TIMESTAMP(3),
            PRIMARY KEY (end_time) NOT ENFORCED
        ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'out-total-flink',
        'properties.bootstrap.servers' = '40.89.150.165:9092',
        'key.format' = 'json',
        'value.format' = 'json'
        )
        """
    st_env.execute_sql(sink_ddl)

    sink_ddl = f"""
        CREATE TABLE grep_sink (
            total BIGINT,
            end_time TIMESTAMP(3),
            PRIMARY KEY (end_time) NOT ENFORCED
        ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'out-grep-flink',
        'properties.bootstrap.servers' = '40.89.150.165:9092',
        'key.format' = 'json',
        'value.format' = 'json'
        )
        """
    st_env.execute_sql(sink_ddl)

    sink_ddl = f"""
        CREATE TABLE topk_sink (
            total BIGINT,
            message STRING,
            end_time TIMESTAMP(3),
            PRIMARY KEY (end_time) NOT ENFORCED
        ) WITH (
        'connector' = 'upsert-kafka',
        'topic' = 'out-topk-flink',
        'properties.bootstrap.servers' = '40.89.150.165:9092',
        'key.format' = 'json',
        'value.format' = 'json'
        )
        """
    st_env.execute_sql(sink_ddl)



if __name__ == "__main__":
    main()
