package uk.ac.warwick;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.SQLException;
import java.util.*;

import static uk.ac.warwick.DBUtils.toArrayNode;

public class TermToDocs {
    public static int[] insertTermDocs(NamedParameterJdbcTemplate namedParameterJdbcTemplate, Map<String, ArrayNode> termDocs) throws SQLException {
        System.out.println("insertTermDocs!!");
        Set<String> terms = termDocs.keySet();
        int n = terms.size();
        System.out.println("N is "+n);
        String sql = "INSERT INTO appledaily_term_docs (term, docs) VALUES (:term, :docs)";

        Map[] maps = new Map[n];
        int i = 0;
        for(String term: terms){
            PGobject obj = new PGobject();
            obj.setType("jsonb");
            obj.setValue(termDocs.get(term).toString());

            Map<String, Object> map = new HashMap();
            map.put("term", term);
            map.put("docs", obj);
            maps[i++] = map;
        }

        return namedParameterJdbcTemplate.batchUpdate(sql, maps);
    }

    public static Map<String, ArrayNode> getTermDocs(NamedParameterJdbcTemplate namedParameterJdbcTemplate, int offset, int limit){
        Map<String, ArrayNode> termDocs = new HashMap();
        ObjectMapper objectMapper = new ObjectMapper();
        String sql = "SELECT * FROM appledaily_jieba OFFSET "+offset+" LIMIT "+limit;
        List<Map<String, Object>> list = namedParameterJdbcTemplate.queryForList(sql, new HashMap());

        list.stream().forEach(map->{
            String docID = map.get("id").toString();
            ArrayNode termArrayNode = toArrayNode(map.get("text").toString());
            Iterator<JsonNode> iterator = termArrayNode.iterator();
            while(iterator.hasNext()){
                String term = iterator.next().asText();
                ArrayNode oriArrayNode = termDocs.getOrDefault(term, objectMapper.createArrayNode());
                oriArrayNode.add(docID);
                termDocs.put(term, oriArrayNode);
            }
        });
        return termDocs;
    }

    public static void run(NamedParameterJdbcTemplate namedParameterJdbcTemplate) throws SQLException {
        int limit = 100;
        int n = 120297;
        Map<String, ArrayNode> termDocs = new HashMap();
        for (int offset = 0; offset <= n; offset+=limit) {
            System.out.println("【"+offset+"/"+n+"】");
            termDocs.putAll(getTermDocs(namedParameterJdbcTemplate, offset, limit));
        }
        insertTermDocs(namedParameterJdbcTemplate, termDocs);
    }
}