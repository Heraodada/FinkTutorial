package com.atguigu.wc;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SteamWordCount {
    public static void main(String[] args) throws Exception {
        //创建一个流式运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //从参数中提取主机名和端口号
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
//        //获取主机名
//        String hostname = parameterTool.get("host");
//        //获取端口号
//        Integer port = parameterTool.getInt("port");

        //读取文本流
        DataStream<String> lineDataStream = env.socketTextStream("hadoop102",7777);

        //转换计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = lineDataStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
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
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyesStream = wordAndOneTuple.keyBy(data -> data.f0);

        //求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyesStream.sum(1);

        //打印
        sum.print();

        //启动执行操作
        env.execute();

    }
}
