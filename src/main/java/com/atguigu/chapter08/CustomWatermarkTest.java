package com.atguigu.chapter08;

import com.atguigu.chapter05.ClickSource;
import com.atguigu.chapter05.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class CustomWatermarkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy()).print();

        env.execute();
    }

    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event>{

        @Override
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                public long extractTimestamp(Event event, long l) {
                    return event.timestamp;//告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }

        @Override
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }

//    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event>{
//        private Long delayTime = 5000L; //延迟时间
//        private Long maxTs = Long.MIN_VALUE + delayTime +1L; // 观察到的最大时间戳
//        @Override
//        public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
//            // 每来一条数据就调用一次
//            maxTs = Math.max(event.timestamp,maxTs);//更新时间戳
//        }
//
//        @Override
//        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
//            // 发射水位线，默认 200ms 调用一次
//            watermarkOutput.emitWatermark(new Watermark(maxTs - delayTime -1L));
//        }
//    }
        public static class CustomPeriodicGenerator implements WatermarkGenerator<Event>{

    @Override
    public void onEvent(Event event, long l, WatermarkOutput watermarkOutput) {
        if (event.name.equals("Mary")){
            watermarkOutput.emitWatermark(new Watermark(event.timestamp-1));
        }
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput watermarkOutput) {

    }
}
}
