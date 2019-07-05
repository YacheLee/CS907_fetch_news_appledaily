package uk.ac.warwick;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.huaban.analysis.jieba.JiebaSegmenter;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;
import static uk.ac.warwick.DBUtils.getDataSource;

public class JiebaSegmentation {
    public int i = 0;
    private NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    private int year;
    private int limit;
    private int offset;
    private static Set<UUID> docs = new HashSet();

    public JiebaSegmentation(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int limit, int offset) {
        this.namedParameterJdbcTemplate = namedParameterJdbcTemplate;
        this.year = year;
        this.limit = limit;
        this.offset = offset;
    }

    public void update() {
        List<Map<String, Object>> newsList = fetchNewsFromDatabase(this.namedParameterJdbcTemplate, this.year, this.limit, this.offset);
        newsList.parallelStream().forEach(map -> {
            UUID id = (UUID) map.get("id");
            if(!docs.contains(id)){
                String text = (String) map.get("text");
                try {
                    Map<String, Long> frequencyMap = toFrequencyMap(getSegmentText(text));
                    insertSeg(namedParameterJdbcTemplate, id, frequencyMap);
                    docs.add(id);
                } catch (Exception ex) {
                    System.out.println(ex);
                    System.out.println("[Error] id=" + id);
                }
            }
        });
    }

    private int[] insertSeg(NamedParameterJdbcTemplate namedParameterJdbcTemplate, UUID id, Map<String, Long> freqMap) throws SQLException {
        if (freqMap != null) {
            String sql = "INSERT INTO appledaily_term_freq (doc_id, term, freq) VALUES (:doc_id, :term, :freq)";

            Set<String> keySet = freqMap.keySet();
            int n = keySet.size();
            Map[] maps = new Map[n];
            int i = 0;
            for (String term: keySet) {
                HashMap<String, Object> map = new HashMap();
                long freq = freqMap.get(term);
                map.put("doc_id", id);
                map.put("term", term);
                map.put("freq", freq);
                maps[i++] = map;
            }
            return namedParameterJdbcTemplate.batchUpdate(sql, maps);
        }
        return null;
    }

    private List<Map<String, Object>> fetchNewsFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year, int limit, int offset) {
        String sql = "select id, text from appledaily\n" +
                "WHERE timestamp >= '" + year + "-01-01'\n" +
                "AND timestamp < '" + year + "-12-31'\n" +
                "LIMIT " + limit + "\n" +
                "OFFSET " + offset;
        Map<String, News> map = new HashMap();
        return namedParameterJdbcTemplate.queryForList(sql, map);
    }

    public static int count(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int year) {
        String sql = "select count(1) as count from appledaily\n" +
                "WHERE timestamp >= '" + year + "-01-01'\n" +
                "AND timestamp < '" + year + "-12-31'";
        Map<String, News> map = new HashMap();
        return Integer.parseInt(namedParameterJdbcTemplate.queryForList(sql, map).get(0).get("count").toString());
    }

    public static ArrayNode toArrayNode(List<String> list) {
        return new ObjectMapper().valueToTree(list);
    }

    public static List<String> getSegmentText(String sentence) {
        JiebaSegmenter obj = new JiebaSegmenter();
        return obj.sentenceProcess(sentence);
    }

    public static Map<String, Long> toFrequencyMap(List<String> list) {
        Map<String, Long> map = new HashMap<>();
        list.stream().collect(groupingBy(k -> k, () -> map, Collectors.counting()));
        return map;
    }

    public static void execute(int year) {
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