package tableAPI;

import java.text.SimpleDateFormat;

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
}
