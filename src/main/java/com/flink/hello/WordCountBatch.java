package com.flink.hello;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Desc 演示Flink-DataSet-API-实现WordCount
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO 1.source
        DataSource<String> lines = env.fromElements("itcast hadoop spark", "itcast hadoop spark", "itcast hadoop", "itcast");

        //TODO 2.transformation
        //切割 -> Tuple2<String, Integer> -> 分组
        /*
        @FunctionalInterface
        public interface FlatMapFunction<T, O> extends Function, Serializable {
            void flatMap(T value, Collector<O> out) throws Exception;
        }
         */
        UnsortedGrouping<Tuple2<String, Integer>> grouped = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //s表示每一行数据
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        }).groupBy(0);


        AggregateOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        //TODO 3.sink
        result.print();
    }
}
