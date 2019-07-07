package uk.ac.warwick;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import static uk.ac.warwick.DBUtils.getDataSource;

public class Main {
    public static void main(String[] args){
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(getDataSource());
        JiebaSentences.run(namedParameterJdbcTemplate);
    }
}