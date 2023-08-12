package com.atguigu.chapter07;

import com.atguigu.chapter.Event;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class sinkToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L),
                new Event("Alice", "./prod?id=100", 3000L),
                new Event("Alice", "./prod?id=200", 3500L),
                new Event("Bob", "./prod?id=2", 2500L),
                new Event("Alice", "./prod?id=300", 3600L),
                new Event("Bob", "./home", 3000L),
                new Event("Bob", "./prod?id=1", 2300L),
                new Event("Bob", "./prod?id=3", 3300L)
        );

        //mysql连接信息
        JdbcConnectionOptions mysqlconnect = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://hadoop102:3306/flinkdatabase")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("root")
                .withPassword("123456")
                .build();

        stream.addSink(
                JdbcSink.sink(
                        "INSERT INTO clicks(user,url) VALUES(?,?)",(statement,r) ->{
                            statement.setString(1,r.user);
                            statement.setString(2,r.url);
                        }, JdbcExecutionOptions.builder()
                                        .withBatchSize(1000)
                                        .withBatchIntervalMs(200)
                                        .withMaxRetries(5)
                                .build(),
                        mysqlconnect

                )
        );

        env.execute();

    }
}
