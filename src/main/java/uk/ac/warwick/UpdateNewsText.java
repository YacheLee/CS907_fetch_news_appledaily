package uk.ac.warwick;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static uk.ac.warwick.DBUtils.getDataSource;
import static uk.ac.warwick.FetchNews.getText;

public class UpdateNewsText {
    public int i = 0;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private int year;
    private int limit;
    private int offset;

    public UpdateNewsText(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int limit, int offset) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.year = year;
        this.limit = limit;
        this.offset = offset;
    }

    public void updateDatabaseText(){
        List<Map<String, Object>> newsList = fetchNewsFromDatabase(this.namedParameterJdbcTemplate, this.year, this.limit, this.offset);
        int n = newsList.size();

        newsList.parallelStream().forEach(map->{
            UUID id = (UUID)map.get("id");
            String html = (String) map.get("html");
            try{
                String text = getText(html);
                updateNewsText(namedParameterJdbcTemplate, id, text);
            }
            catch (Exception ex){
                System.out.println("[Error] id="+id);
            }
        });
    }

    private int updateNewsText(NamedParameterJdbcTemplate namedParameterJdbcTemplate, UUID id, String text){
        if(text!=null){
            String sql = "UPDATE AppleDaily SET text=:text WHERE id = :id";
            HashMap<String, Object> map = new HashMap();
            map.put("id", id);
            map.put("text", text);
            return namedParameterJdbcTemplate.update(sql, map);
        }
        return 0;
    }

    private List<Map<String, Object>> fetchNewsFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int limit, int offset){
        String sql = "select id, html from appledaily\n" +
                "WHERE timestamp >= '"+year+"-01-01'\n" +
                "AND timestamp < '"+year+"-12-31'\n" +
                "AND text IS NULL\n" +
                "LIMIT "+limit+"\n" +
                "OFFSET "+offset;
        Map<String, News> map = new HashMap();
        return namedParameterJdbcTemplate.queryForList(sql, map);
    }

    public static int count(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year){
        String sql = "select count(1) as count from appledaily\n" +
                "WHERE timestamp >= '"+year+"-01-01'\n" +
                "AND timestamp < '"+year+"-12-31'\n" +
                "AND text IS NULL";
        Map<String, News> map = new HashMap();
        return Integer.parseInt(namedParameterJdbcTemplate.queryForList(sql, map).get(0).get("count").toString());
    }

    public static void execute(int year){
        DataSource dataSource = getDataSource();
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        int limit = 100;
        int n = UpdateNewsText.count(namedParameterJdbcTemplate, year);

        for (int offset = 0; offset <= n; offset+=limit) {
            System.out.println("【"+offset+"/"+n+"】");
            UpdateNewsText updateNewsText = new UpdateNewsText(namedParameterJdbcTemplate, year, limit, offset);
            updateNewsText.updateDatabaseText();
        }
    }
}
