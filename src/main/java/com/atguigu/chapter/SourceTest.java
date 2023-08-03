package com.atguigu.chapter;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.ArrayList;
import java.util.Properties;


public class SourceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment evn = StreamExecutionEnvironment.getExecutionEnvironment();
        //直接从集合中读取数据
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(1);
        nums.add(2);
        nums.add(3);
        DataStreamSource<Integer> stream = evn.fromCollection(nums);

        //从集合中读取
        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Tom","./home",1000L));
        events.add(new Event("Jay","./cart",2000L));
        DataStreamSource<Event> stream1 = evn.fromCollection(events);

        //从元素中读取数据
        DataStreamSource<Event> stream2 = evn.fromElements(
                new Event("Tom", "./home", 1000L),
                new Event("Tom", "./home", 1000L)

        );

        //从socket文本流中读取
        DataStreamSource<String> stream3 = evn.socketTextStream("172.30.6.218", 7777);

        //从kafka读取
        Properties properties = new Properties();
        //指令连接
        properties.setProperty("bootstrap.servers","192.168.10.100:9092");
        //指定id
        properties.setProperty("group.id","consumer-group");
        //消费者KV反序列化
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StrinDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StrinDeserializer");
        //指定偏移量从末尾开始读
        properties.setProperty("auto.offset.reset","latest");

        DataStreamSource<String> streamkafka = evn.addSource(new FlinkKafkaConsumer<String>(
                "clinks",
                new SimpleStringSchema(),
                properties
        ));

        //stream.print();
        //stream1.print();
        //stream2.print();
        //stream3.print();
        streamkafka.print("kafka");

        //执行
        evn.execute();

    }
}
