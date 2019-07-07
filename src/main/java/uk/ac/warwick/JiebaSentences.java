package uk.ac.warwick;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.toList;
import static uk.ac.warwick.DBUtils.toArrayNode;

public class JiebaSentences {
    public static List<Map<String, Object>> getTermDocs(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int offset, int limit) {
        String sql = "SELECT * FROM appledaily_term_docs ORDER BY term LIMIT " + limit + " OFFSET " + offset;
        return namedParameterJdbcTemplate.queryForList(sql, new HashMap());
    }

    public static List<ArrayNode> getJiebaSentences(NamedParameterJdbcTemplate namedParameterJdbcTemplate, ArrayNode docs) {
        String inString = docs.toString().replace("[", "").replace("]", "").replaceAll("\"", "'");
        String sql = "select * from appledaily_jieba WHERE id in (" + inString + ")";
        List<Map<String, Object>> list = namedParameterJdbcTemplate.queryForList(sql, new HashMap());
        List<ArrayNode> sentences = list.stream().map(e -> toArrayNode(e.get("text").toString())).collect(toList());
        return sentences;
    }

    public static void insertTermSentence(NamedParameterJdbcTemplate namedParameterJdbcTemplate, Map<String, List> termSentences) throws SQLException {
        Set<String> terms = termSentences.keySet();
        int n = terms.size();
        String sql = "INSERT INTO appledaily_jieba_sentences (term, jieba_sentences) VALUES (:term, :sentences)";

        Map[] maps = new Map[n];
        int i = 0;
        for (String term : terms) {
            PGobject obj = new PGobject();
            obj.setType("jsonb");
            obj.setValue(termSentences.get(term).toString());

            Map<String, Object> map = new HashMap();
            map.put("term", term);
            map.put("sentences", obj);
            maps[i++] = map;
        }
        namedParameterJdbcTemplate.batchUpdate(sql, maps);
    }

    public static void run(NamedParameterJdbcTemplate namedParameterJdbcTemplate){
        int limit = 100;
        int n = 951316;

        for (int offset = 0; offset <= n; offset += limit) {
            run(namedParameterJdbcTemplate, n, offset, limit);
        }
    }

    public static void run(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int n, int offset, int limit){
        System.out.println("【" + offset + "/" + n + "】");
        List<Map<String, Object>> termDocs = getTermDocs(namedParameterJdbcTemplate, offset, limit);

        Map<String, List> termSentences = new HashMap();
        for (Map map : termDocs) {
            String term = map.get("term").toString();
            ArrayNode docs = toArrayNode(map.get("docs").toString());
            List<ArrayNode> jiebaSentences = getJiebaSentences(namedParameterJdbcTemplate, docs);
            termSentences.put(term, jiebaSentences);
        }
        try {
            insertTermSentence(namedParameterJdbcTemplate, termSentences);
        } catch (Exception ex) {
            try {
                Files.write(Paths.get("C:\\Users\\Scott\\CS907_fetch_news_appledaily\\src\\main\\resources\\term_docs_error2.txt"), (offset+"\n").getBytes(), StandardOpenOption.APPEND);
            }catch (IOException e) {
                System.out.println(e);
            }
        }
    }
}