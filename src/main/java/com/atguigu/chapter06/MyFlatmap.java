package com.atguigu.chapter06;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

public class MyFlatmap extends RichMapFunction {
    @Override
    public void open(Configuration parameters) throws Exception {
        // 做一些初始化工作
        // 例如建立一个和 MySQL 的连接
    }

    @Override
    public Object map(Object o) throws Exception {
        // 对数据库进行读写
        return null;
    }

    @Override
    public void close() throws Exception {
        // 清理工作，关闭和 MySQL 数据库的连接。
    }
}
