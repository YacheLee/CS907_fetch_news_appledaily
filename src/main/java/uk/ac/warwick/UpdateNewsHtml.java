package uk.ac.warwick;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.jsoup.nodes.Document;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class UpdateNewsHtml {
    public int i = 0;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private int year;

    public UpdateNewsHtml(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.year = year;
    }

    public void updateDatabaseHtml(){
        List<Map<String, Object>> newsList = fetchNewsFromDatabase(this.namedParameterJdbcTemplate, this.year);
        int n = newsList.size();
        newsList.parallelStream()
                .forEach(map->{
                    System.out.println("["+year+"] "+(++this.i)+"/"+n);
                    UUID id = (UUID)map.get("id");
                    String url = (String)map.get("url");
                    try {
                        Document doc = FetchNews.getDoc(url);
                        String html = FetchNews.getHtml(doc);
                        updateNewsHtml(namedParameterJdbcTemplate, id, html);
                    } catch (IOException e) {
                        System.out.println("ERROR: ["+year+"], "+id);
                        e.printStackTrace();
                    }
                });
    }

    private int updateNewsHtml(NamedParameterJdbcTemplate namedParameterJdbcTemplate, UUID id, String html){
        String sql = "UPDATE AppleDaily SET html=:html WHERE id = :id";
        HashMap<String, Object> map = new HashMap();
        map.put("id", id);
        map.put("html", html);
        return namedParameterJdbcTemplate.update(sql, map);
    }

    private List<Map<String, Object>> fetchNewsFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year){
        String sql = "select id, url from appledaily\n" +
                "WHERE timestamp >= '"+year+"-01-01'\n" +
                "AND timestamp < '"+year+"-12-31'\n" +
                "AND html IS NULL";
        Map<String, News> map = new HashMap();
        return namedParameterJdbcTemplate.queryForList(sql, map);
    }

    private void fetchNewsList(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year){
        for (int month = 1; month <= 12; month++) {
            fetchNewsList(namedParameterJdbcTemplate, year, month);
        }
    }

    public void fetchNewsList(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int month) {
        for (int day = 1; day <= DateUtils.getEndDateOfMonth(year, month); day++) {
            for (int page = 1; ; page++) {
                try {
                    System.out.println("[" + year + ", " + month + ", " + day + ", " + page + "]...");
                    ArrayNode newsList = FetchNewsList.get(year + "/" + month + "/" + day, page);
                    if (newsList.size() == 0) {
                        System.out.println("The size of page " + page + " is zero");
                        break;
                    }
                    insertNewsList(namedParameterJdbcTemplate, newsList);
                } catch (Exception ex) {
                    System.out.println("[" + year + ", " + month + ", " + day + " , " + page + "] doesn't work well...");
                    System.out.println(ex);
                }
            }
        }
    }

    public void insertNewsList(NamedParameterJdbcTemplate namedParameterJdbcTemplate, ArrayNode newsList) throws ParseException {
        Iterator iterator = newsList.iterator();
        int n = newsList.size();
        News[] newsArr = new News[n];
        int i = 0;
        while (iterator.hasNext()) {
            News news = new News();
            ObjectNode objectNode = (ObjectNode) iterator.next();
            news.setTimestamp(DateUtils.parseToTimeStamp(objectNode.get("pubDate").asText()));
            news.setUrl(objectNode.get("sharing").get("url").asText());
            news.setTitle(objectNode.get("title").asText());
            newsArr[i] = news;
            i++;
        }
        DBUtils.insertNews(namedParameterJdbcTemplate, newsArr);
    }
}
