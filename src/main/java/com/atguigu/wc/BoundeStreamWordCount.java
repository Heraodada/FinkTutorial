package com.atguigu.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class BoundeStreamWordCount {
    public static void main(String[] args) throws Exception {
        //创建一个流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //读取文件
        DataStreamSource<String> lineDataStreamSource = env.readTextFile("input/word.txt");

        //转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStreamSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    //将每一行文本进行分词
                    String[] words = line.split(" ");
                    //将每个单词转换成二元组输出
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                })
                //类型擦除
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        //分组
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);

        //打印
        sum.print();

        //启动执行操作
        env.execute();
    }
}
