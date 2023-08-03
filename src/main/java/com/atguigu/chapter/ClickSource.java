package com.atguigu.chapter;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements ParallelSourceFunction<Event> {
    //定义一个标签
    private Boolean running = true;
    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();//指定数据集随机选取数据
        String[] users = {"Mary","Alice","Bob","cARY"};
        String[] urls = {"./home","./cart","./fav","./prod?id=1","./prod?id=2"};
        //循环随机生成数据
        while (running){
            sourceContext.collect(new Event(users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    Calendar.getInstance().getTimeInMillis()));
            //睡一秒
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
