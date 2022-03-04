package com.flink.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * Desc 演示Flink-Connectors-KafkaComsumer/Source + KafkaProducer/Sink
 */
public class KafkaSinkDemo {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.3.88:9092");//集群地址
        props.setProperty("group.id", "console-consumer-48062");//消费者组id
        props.setProperty("auto.offset.reset", "latest");//latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费 /earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true");//自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("cdm_order_cube_detail_test", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        //TODO 2.transformation
        /*SingleOutputStreamOperator<String> etlDS = kafkaDS.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return value.contains("success");
            }
        });

        //TODO 3.sink
        etlDS.print();*/
        kafkaDS.print();

        Properties props2 = new Properties();
        props2.setProperty("bootstrap.servers", "192.168.3.88:9092");
        FlinkKafkaProducer<String> kafkaSink = new FlinkKafkaProducer<>("cdm_order_cube_detail_test2", new SimpleStringSchema(), props2);
        kafkaDS.addSink(kafkaSink);

        //TODO 4.execute
        env.execute();
    }
}
