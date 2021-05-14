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
    register_transactions_sink_into_csv(st_env)

    #Filter
    # st_env.from_path("source") \
    #     .window(Slide.over(lit(2).minutes).every(lit(1).minutes).on("rowtime").alias("w")) \
    #     .group_by("customer_id, w") \
    #     .select("""customer_id as customer_id,
    #              count(*) as total_counts,
    #              w.start as start_time,
    #              w.end as end_time
    #              """) \
    #     .insert_into("sink_into_csv")

        # .window(Tumble.over("10.hours").on("rowtime").alias("w")) \

        # .select("""customer as customer, 
        #            count(transaction_type) as count_transactions,
        #            sum(online_payment_amount) as total_online_payment_amount, 
        #            sum(in_store_payment_amount) as total_in_store_payment_amount,
        #            last(lat) as lat,
        #            last(lon) as lon,
        #            w.end as last_transaction_time
        #            """) \
        # .group_by("customer, w") \

    # st_env.from_path("source") \
    #     .window(Slide.over(lit(2).minutes).every(lit(1).minutes).on("rowtime").alias("w")) \
    #     .print()


#    st_env.from_path("source_tbl") \
#        .window(Slide.over(lit(1).minute).every(lit(5).seconds).on("ts").alias("w")) \
#        .group_by(col("w")) \
#        .select("""count(message) as total,
#                    w.end as end_time
#                   """) \
#        .insert_into("total_sink")

#    st_env.from_path("source_tbl") \
#        .where("message = 'dolorem'") \
#        .window(Slide.over(lit(1).minute).every(lit(5).seconds).on("ts").alias("w")) \
#        .group_by(col("w")) \
#        .select("""
#                    count(message) as total,
#                    w.end as end_time
#                   """) \
#        .insert_into("grep_sink")

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

    # table_env = TableEnvironment.create(settings)

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
    # st_env.connect(Kafka()
    #                .version("universal")
    #                .topic("quickstart-events")
    #                .start_from_latest()
    #                .property("bootstrap.servers", "40.89.150.165:9092")) \
    #     .with_format(Json()
    #     .fail_on_missing_field(True)
    #     .schema(DataTypes.ROW([
    #     DataTypes.FIELD("customer", DataTypes.STRING()),
    #     DataTypes.FIELD("transaction_type", DataTypes.STRING()),
    #     DataTypes.FIELD("online_payment_amount", DataTypes.DOUBLE()),
    #     DataTypes.FIELD("in_store_payment_amount", DataTypes.DOUBLE()),
    #     DataTypes.FIELD("lat", DataTypes.DOUBLE()),
    #     DataTypes.FIELD("lon", DataTypes.DOUBLE()),
    #     DataTypes.FIELD("transaction_datetime", DataTypes.TIMESTAMP(3))]))) \
    #     .with_schema(Schema()
    #     .field("customer", DataTypes.STRING())
    #     .field("transaction_type", DataTypes.STRING())
    #     .field("online_payment_amount", DataTypes.DOUBLE())
    #     .field("in_store_payment_amount", DataTypes.DOUBLE())
    #     .field("lat", DataTypes.DOUBLE())
    #     .field("lon", DataTypes.DOUBLE())
    #     .field("rowtime", DataTypes.TIMESTAMP(3))
    #     .rowtime(
    #     Rowtime()
    #         .timestamps_from_field("rowtime")
    #         .watermarks_periodic_bounded(60000))) \
    #     .in_append_mode() \
    #     .create_temporary_table("source")


    # Add Source
            # .property("security.protocol", "SASL_PLAINTEXT") \
            # .property("sasl.mechanism", "PLAIN") \
            # .property("sasl.jaas.config", "<user,password>") \



def register_transactions_sink_into_csv(st_env):
    # result_file = "/app/src/output_file.csv"
    # if os.path.exists(result_file):
    #     os.remove(result_file)


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


    # st_env.register_table_sink("sink_into_csv",
    #                            CsvTableSink(["total",
    #                                          ],
    #                                         [DataTypes.BIGINT()],
    #                         #    CsvTableSink(["customer",
    #                         #                  "count_transactions",
    #                         #                  "total_online_payment_amount",
    #                         #                  "total_in_store_payment_amount",
    #                         #                  "lat",
    #                         #                  "lon",
    #                         #                  "last_transaction_time"],
    #                         #                 [DataTypes.STRING(),
    #                         #                  DataTypes.DOUBLE(),
    #                         #                  DataTypes.DOUBLE(),
    #                         #                  DataTypes.DOUBLE(),
    #                         #                  DataTypes.DOUBLE(),
    #                         #                  DataTypes.DOUBLE(),
    #                         #                  DataTypes.TIMESTAMP(3)],
    #                                         result_file))

    # result_file = "/app/src/output_file.csv"
    # if os.path.exists(result_file):
    #     os.remove(result_file)
    # env.register_table_sink("sink_into_csv",
    #                         CsvTableSink(["customer_id",
    #                                       "total_count",
    #                                       "start_time",
    #                                       "end_time"],
    #                                      [DataTypes.STRING(),
    #                                       DataTypes.DOUBLE(),
    #                                       DataTypes.TIMESTAMP(3),
    #                                       DataTypes.TIMESTAMP(3)],
    #                                      result_file))

if __name__ == "__main__":
    main()
