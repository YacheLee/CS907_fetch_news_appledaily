package uk.ac.warwick;

import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.sql.SQLException;

import static uk.ac.warwick.DBUtils.getDataSource;

public class Main {
    public static void main(String[] args) throws SQLException {
        NamedParameterJdbcTemplate namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(getDataSource());
        TermToDocs.run(namedParameterJdbcTemplate);
    }
}