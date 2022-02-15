package com.flink.sink;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc 演示Flink官方提供的JdbcSink
 */
public class JDBCSinkDemo {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStreamSource<Student> studentDS = env.fromElements(new Student(null, "tony2", 18));
        //TODO 2.transformation
        //TODO 3.sink
        studentDS.addSink(JdbcSink.sink(
                "INSERT INTO `t_student` (`id`, `name`, `age`) VALUES (null, ?, ?)",
                (ps, value) -> {
                    ps.setString(1, value.getName());
                    ps.setInt(2, value.getAge());
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/bigdata")
                        .withUsername("root")
                        .withPassword("root")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .build()
        ));

        //TODO 4.execute
        env.execute();
    }


    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Student {
        private Integer id;
        private String name;
        private Integer age;
    }

}
