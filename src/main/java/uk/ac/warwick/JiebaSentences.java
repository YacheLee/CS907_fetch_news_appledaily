package uk.ac.warwick;

import com.fasterxml.jackson.databind.node.ArrayNode;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.SQLException;
import java.util.*;

import static java.util.stream.Collectors.toList;
import static uk.ac.warwick.DBUtils.toArrayNode;

public class JiebaSentences {
    public static List<Map<String, Object>> getTermDocs(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int offset, int limit){
        String sql = "SELECT * FROM appledaily_term_docs LIMIT "+limit+" OFFSET "+offset;
        return namedParameterJdbcTemplate.queryForList(sql, new HashMap());
    }

    public static List<ArrayNode> getJiebaSentences(NamedParameterJdbcTemplate namedParameterJdbcTemplate, ArrayNode docs){
        String inString = docs.toString().replace("[", "").replace("]", "").replaceAll("\"", "'");
        String sql = "select * from appledaily_jieba WHERE id in ("+inString+")";
        List<Map<String, Object>> list = namedParameterJdbcTemplate.queryForList(sql, new HashMap());
        List<ArrayNode> sentences = list.stream().map(e -> toArrayNode(e.get("text").toString())).collect(toList());
        return sentences;
    }

    public static int[] insertTermSentence(NamedParameterJdbcTemplate namedParameterJdbcTemplate, Map<String, List> termSentences) throws SQLException {
        System.out.println("insertTermSentence!!");
        Set<String> terms = termSentences.keySet();
        int n = terms.size();
        System.out.println("N is "+n);
        String sql = "INSERT INTO appledaily_jieba_sentences (term, jieba_sentences) VALUES (:term, :sentences)";

        Map[] maps = new Map[n];
        int i = 0;
        for(String term: terms){
            PGobject obj = new PGobject();
            obj.setType("jsonb");
            obj.setValue(termSentences.get(term).toString());

            Map<String, Object> map = new HashMap();
            map.put("term", term);
            map.put("sentences", obj);
            maps[i++] = map;
        }
        return namedParameterJdbcTemplate.batchUpdate(sql, maps);
    }

    public static void run(NamedParameterJdbcTemplate namedParameterJdbcTemplate) throws SQLException {
        int limit = 100;
        int n = 951316;

        List<Map<String, Object>> termDocs = new ArrayList();
        for (int offset = 0; offset <= n; offset+=limit) {
            System.out.println("【"+offset+"/"+n+"】");
            termDocs.addAll(getTermDocs(namedParameterJdbcTemplate, offset, limit));
        }

        Map<String, List> termSentences = new HashMap();
        for(Map map: termDocs){
            String term = map.get("term").toString();
            ArrayNode docs = toArrayNode(map.get("docs").toString());
            List<ArrayNode> jiebaSentences = getJiebaSentences(namedParameterJdbcTemplate, docs);
            termSentences.put(term, jiebaSentences);
        }
        insertTermSentence(namedParameterJdbcTemplate, termSentences);
    }
}