package com.atguigu.chapter05;

import  org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置线程数
        env.setParallelism(1);

        //读取文件流
        DataStreamSource<String> stream1 = env.readTextFile("input/clicks.txt");

        //直接从集合中读取文件
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numStream = env.fromCollection(nums);


        //从集合对象中读取文件
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","./home",1000L));
        events.add(new Event("Bob","./cart",2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);

        //从元素中读取数据
        DataStreamSource<Event> stream3 = env.fromElements(
                new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L)
        );

        //从socket文本流中读取
        DataStreamSource<String> stream4 = env.socketTextStream("localhost", 7777);

//        stream1.print("1");
//        numStream.print("nums");
//        stream2.print("2");
//        stream3.print("3");
//        stream4.print("4");

        Properties properties = new Properties();
        //指令连接
        properties.setProperty("bootstrap.servers","hadoop102:9092");
        //指定groupid
        properties.setProperty("group.id","comsumer-group");
        //消费者k-v反序列化
        properties.setProperty("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        //指定偏移量读取（末尾）
        properties.setProperty("auto.offset.reset","latest");


        //从kafka消费数据
        DataStreamSource<String> kafkaSteram = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), properties));
        kafkaSteram.print("kafka");


        //启动环境
        env.execute();
    }
}
