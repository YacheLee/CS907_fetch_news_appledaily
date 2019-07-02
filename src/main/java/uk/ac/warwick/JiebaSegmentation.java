package uk.ac.warwick;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.huaban.analysis.jieba.JiebaSegmenter;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static uk.ac.warwick.DBUtils.getDataSource;

public class JiebaSegmentation {
    public int i = 0;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private int year;
    private int limit;
    private int offset;

    public JiebaSegmentation(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int limit, int offset) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.year = year;
        this.limit = limit;
        this.offset = offset;
    }

    public void update(){
        List<Map<String, Object>> newsList = fetchNewsFromDatabase(this.namedParameterJdbcTemplate, this.year, this.limit, this.offset);
        newsList.parallelStream().forEach(map->{
            UUID id = (UUID)map.get("id");
            String text = (String) map.get("text");
            try{
                ArrayNode segmentText = toArrayNode(getSegmentText(text));
                insertSeg(namedParameterJdbcTemplate, id, segmentText);
            }
            catch (Exception ex){
                System.out.println(ex);
                System.out.println("[Error] id="+id);
            }
        });
    }

    private int insertSeg(NamedParameterJdbcTemplate namedParameterJdbcTemplate, UUID id, ArrayNode arrayNode) throws SQLException {
        if(arrayNode!=null){
            PGobject obj = new PGobject();
            obj.setType("jsonb");
            obj.setValue(arrayNode.toString());

            String sql = "INSERT INTO appledaily_seg (id, json) VALUES (:id, :json)";
            HashMap<String, Object> map = new HashMap();
            map.put("id", id);
            map.put("json", obj);
            return namedParameterJdbcTemplate.update(sql, map);
        }
        return 0;
    }

    private List<Map<String, Object>> fetchNewsFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int limit, int offset){
        String sql = "select id, text from appledaily\n" +
                "WHERE timestamp >= '"+year+"-01-01'\n" +
                "AND timestamp < '"+year+"-12-31'\n" +
                "LIMIT "+limit+"\n" +
                "OFFSET "+offset;
        Map<String, News> map = new HashMap();
        return namedParameterJdbcTemplate.queryForList(sql, map);
    }

    public static int count(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year){
        String sql = "select count(1) as count from appledaily\n" +
                "WHERE timestamp >= '"+year+"-01-01'\n" +
                "AND timestamp < '"+year+"-12-31'";
        Map<String, News> map = new HashMap();
        return Integer.parseInt(namedParameterJdbcTemplate.queryForList(sql, map).get(0).get("count").toString());
    }

    public static ArrayNode toArrayNode(List<String> list){
        return new ObjectMapper().valueToTree(list);
    }

    public static List<String> getSegmentText(String text){
        return new JiebaSegmenter().sentenceProcess(text);
    }

    public static void execute(int year){
        DataSource dataSource = getDataSource();
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        int limit = 100;
        int n = count(namedParameterJdbcTemplate, year);

        for (int offset = 0; offset <= n; offset+=limit) {
            System.out.println("【"+offset+"/"+n+"】");
            JiebaSegmentation obj = new JiebaSegmentation(namedParameterJdbcTemplate, year, limit, offset);
            obj.update();
        }
    }
}