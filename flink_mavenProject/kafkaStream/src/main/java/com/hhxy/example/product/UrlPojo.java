package com.hhxy.example.product;

import java.sql.Timestamp;

public class UrlPojo {
    public String url;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public UrlPojo() {
    }

    public UrlPojo(String url, Long count, Long windowStart, Long windowEnd) {
        this.url = url;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "UrlPojo{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
