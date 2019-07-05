package uk.ac.warwick;

import org.postgresql.util.PGobject;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static uk.ac.warwick.DBUtils.toArrayNode;

public class TermToDocs {
    static int i = 0;
    public static List<String> getDistinctTermsFromDatabase(NamedParameterJdbcTemplate namedParameterJdbcTemplate) {
        String sql = "SELECT distinct(term) as term from appledaily_term_freq";
        Map<String, Object> map = new HashMap();
        List<Map<String, Object>> maps = namedParameterJdbcTemplate.queryForList(sql, map);
        return maps.parallelStream().map(obj -> obj.get("term").toString()).collect(toList());
    }

    public static List<String> writeAndGetDistinctTerms(File file, NamedParameterJdbcTemplate namedParameterJdbcTemplate) throws IOException {
        Charset utf8 = StandardCharsets.UTF_8;
        List<String> distinctTerms = getDistinctTermsFromDatabase(namedParameterJdbcTemplate);
        if (file.exists()) {
            file.delete();
        }
        Files.write(Paths.get(file.getAbsolutePath()), distinctTerms, utf8);
        return distinctTerms;
    }

    public static List<String> readDistinctTerms(File file){
        try (Stream<String> stream = Files.lines(Paths.get(file.getAbsolutePath()))) {
            return stream
                    .map(String::toUpperCase)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            return null;
        }
    }

    public static List<String> getDistinctTerms(File file, NamedParameterJdbcTemplate namedParameterJdbcTemplate) throws IOException {
        List<String> distinctTerms = file.exists() ? readDistinctTerms(file): writeAndGetDistinctTerms(file, namedParameterJdbcTemplate);
        return distinctTerms;
    }

    public static void writeTermDocs(List<String> distinctTerms, NamedParameterJdbcTemplate namedParameterJdbcTemplate){
        String sql = "Select doc_id From appledaily_term_freq WHERE term = :term";

        int n = distinctTerms.size();
        distinctTerms.parallelStream().forEach(term->{
            System.out.println("【"+(i++)+"/"+n+"】");
            Map<String, Object> map = new HashMap();
            map.put("term", term);
            List<String> docIds = namedParameterJdbcTemplate.queryForList(sql, map).parallelStream().map(obj -> obj.get("doc_id").toString()).collect(toList());
            try {
                insertTermDoc(namedParameterJdbcTemplate, term, docIds);
            } catch (SQLException ex) {
                System.out.println("[Error] term=" + term);
                System.out.println(ex);
            }
        });
    }

    private static int insertTermDoc(NamedParameterJdbcTemplate namedParameterJdbcTemplate, String term, List<String> docs) throws SQLException {
        PGobject obj = new PGobject();
        obj.setType("jsonb");
        obj.setValue(toArrayNode(docs).toString());

        String sql = "INSERT INTO appledaily_term_docs (term, docs) VALUES (:term, :docs)";
        Map<String, Object> map = new HashMap();
        map.put("term", term);
        map.put("docs", obj);

        return namedParameterJdbcTemplate.update(sql, map);
    }
}