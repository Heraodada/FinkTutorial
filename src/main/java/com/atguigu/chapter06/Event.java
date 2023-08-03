package com.atguigu.chapter06;

import java.sql.Timestamp;

public class Event {
   public String name;
   public String url;
   public Long timestamp;

    public Event() {
    }

    public Event(String name, String url, Long timestamp) {
        this.name = name;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
