package io.prestosql.jdbc;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.sql.*;
import java.util.Set;

import static java.lang.String.format;

public class TestBigConnectCloud {
    @Test
    public void testConnect() throws SQLException {
        try (Connection connection = createConnection("470543098948008", "")) {
            Set<String> tables = listTables(connection);
            tables.forEach(System.out::println);
        }
    }

    private Connection createConnection(String tenantId, String extra)
            throws SQLException {
        String url = format("jdbc:datacharm://dsql-%s.cloud.bigconnect.io:443/bigconnect/public?%s", tenantId, extra);
        return DriverManager.getConnection(url, tenantId, tenantId);
    }

    private static Set<String> listTables(Connection connection)
            throws SQLException
    {
        ImmutableSet.Builder<String> set = ImmutableSet.builder();
        try (Statement statement = connection.createStatement();
             ResultSet rs = statement.executeQuery("SHOW TABLES")) {
            while (rs.next()) {
                set.add(rs.getString(1));
            }
        }
        return set.build();
    }
}
