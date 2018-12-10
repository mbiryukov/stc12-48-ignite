package ru.innopolis.ignite2;

import java.sql.*;

public class SqlApache {
    public SqlApache() {
        try {
            setupSql();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private void setupSql() throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
             Statement stmt = conn.createStatement();) {
            stmt.executeUpdate("CREATE TABLE City (id LONG PRIMARY KEY, name VARCHAR) " +
                    "WITH \"template=replicated\"");
            stmt.executeUpdate("CREATE TABLE Person (id LONG, name VARCHAR, city_id LONG, " +
                    "PRIMARY KEY (id, city_id)) WITH \"backups=1, affinityKey=city_id\"");
            stmt.executeUpdate("CREATE INDEX idx_city_name ON City (name)");
            stmt.executeUpdate("CREATE INDEX idx_person_name ON Person (name)");
        }
    }

    public void insertData() {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO City (id, name) VALUES (?, ?)")) {

            stmt.setLong(1, 1L);
            stmt.setString(2, "Forest Hill");
            stmt.executeUpdate();

            stmt.setLong(1, 2L);
            stmt.setString(2, "Denver");
            stmt.executeUpdate();

            stmt.setLong(1, 3L);
            stmt.setString(2, "St. Petersburg");
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
             PreparedStatement stmt = conn.prepareStatement("INSERT INTO Person (id, name, city_id) VALUES (?, ?, ?)")) {

            stmt.setLong(1, 1L);
            stmt.setString(2, "John Doe");
            stmt.setLong(3, 3L);
            stmt.executeUpdate();

            stmt.setLong(1, 2L);
            stmt.setString(2, "Jane Roe");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();

            stmt.setLong(1, 3L);
            stmt.setString(2, "Mary Major");
            stmt.setLong(3, 1L);
            stmt.executeUpdate();

            stmt.setLong(1, 4L);
            stmt.setString(2, "Richard Miles");
            stmt.setLong(3, 2L);
            stmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void getData() {
        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/");
             Statement stmt = conn.createStatement();) {
            try (ResultSet rs = stmt.executeQuery("SELECT p.name, c.name from Person p, City c where p.city_id = c.id")) {
                while (rs.next()) {
                    System.out.println(rs.getString(1) + ", " + rs.getString(2));
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
