package com.hhxy.example.chapter07;

import java.sql.Timestamp;

/**
 * 实现的功能:用户点击某一个网页
 */
public class Event {
    public String user;  //用户
    public String url;  //网页的地址
    public Long timestamp;

    public Event() {
    }

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
