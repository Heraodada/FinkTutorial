package com.atguigu.chapter06;

import com.atguigu.chapter.ClickSource;
import com.atguigu.chapter.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

public class TransReduceTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                //将Event数据类型转换为元组类型
                .map(new MapFunction<Event, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String, Long> map(Event event) throws Exception {
                        return Tuple2.of(event.user,1L);
                    }
                }).keyBy(r -> r.f0)//使用用户名来分流
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0,value1.f1+value2.f1);
                    }
                }).keyBy(r -> true)//为每一条数据分配同一个key,将聚合结果发送到一条流中去
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        // 将累加器更新为当前最大的 pv 统计值，然后向下游发送累加器的值
                        return value1.f1>value2.f1 ? value1:value2;
                    }
                }).print();
        env.execute();
    }
}
