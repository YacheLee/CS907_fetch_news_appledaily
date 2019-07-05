package uk.ac.warwick;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static uk.ac.warwick.DBUtils.getDataSource;
import static uk.ac.warwick.TermToDocs.getDistinctTerms;
import static uk.ac.warwick.TermToDocs.writeTermDocs;

public class Main {
    public static void main(String[] args) throws IOException {
        DataSource dataSource = getDataSource();
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(dataSource);
        String filePath = "C:\\Users\\Scott\\CS907_fetch_news_appledaily\\src\\main\\java\\uk\\ac\\warwick\\distinct_terms.txt";
        List<String> distinctTerms = getDistinctTerms(new File(filePath), namedParameterJdbcTemplate);
        writeTermDocs(distinctTerms, namedParameterJdbcTemplate);
    }
}