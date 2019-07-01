package uk.ac.warwick;

import java.sql.Timestamp;

public class News {
    Timestamp timestamp;
    String url;
    String title;
    String html;
    String text;

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public void setHtml(String html) {
        this.html = html;
    }

    public void setText(String text) {
        this.text = text;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public String getUrl() {
        return url;
    }

    public String getTitle() {
        return title;
    }

    public String getHtml() {
        return html;
    }

    public String getText() {
        return text;
    }
}
