package uk.ac.warwick;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class DBUtils {
    final static String url = "jdbc:postgresql://localhost:5432/CS907";
    final static String user = "postgres";
    final static String password = "postgres*";

    public static int[] insertNews(NamedParameterJdbcTemplate namedParameterJdbcTemplate, News[] newsList) {
        String sql = "INSERT INTO AppleDaily (id, timestamp, url, html, text, title) VALUES (:id, :timestamp, :url, :html, :text, :title)";
        int n = newsList.length;
        Map[] maps = new Map[n];
        for (int i = 0; i < n; i++) {
            News news = newsList[i];
            Map<String, Object> map = new HashMap();
            map.put("id", UUID.randomUUID());
            map.put("timestamp", news.getTimestamp());
            map.put("url", news.getUrl());
            map.put("title", news.getTitle());
            map.put("html", news.getHtml());
            map.put("text", news.getText());
            maps[i] = map;
        }
        return namedParameterJdbcTemplate.batchUpdate(sql, maps);
    }

    public static DataSource getDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setUrl(url);
        dataSource.setUsername(user);
        dataSource.setPassword(password);
        return dataSource;
    }
}
