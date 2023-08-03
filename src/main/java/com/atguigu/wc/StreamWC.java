package com.atguigu.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


public class StreamWC {
    public static void main(String[] args) throws Exception {
        //获取流处理上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //读取文件
        DataStreamSource<String> lineDSS = env.socketTextStream("172.30.6.218",7777);
        //结构转化
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lineDSS.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(Tuple2.of(word, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));
        //分阻
        KeyedStream<Tuple2<String, Long>, String> wordAndSUM = wordAndOne.keyBy(data -> data.f0);
        //聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndSUM.sum(1);
        //输出
        sum.print();
        //执行
        env.execute();
    }
}
