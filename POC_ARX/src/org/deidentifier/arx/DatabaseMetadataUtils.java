package org.deidentifier.arx;

import java.sql.*;
import java.util.*;

// automated methods for detecting primary and foreign keys
public class DatabaseMetadataUtils {

    private static Set<String> getAllTableNames(Connection conn) throws SQLException {
        Set<String> tableNames = new HashSet<>();
        DatabaseMetaData meta = conn.getMetaData();
        ResultSet rs = meta.getTables(null, conn.getCatalog(), "%", new String[]{"TABLE"});
        while (rs.next()) {
            tableNames.add(rs.getString("TABLE_NAME").toLowerCase());
        }
        return tableNames;
    }

    public static Map<String, TableKeys> getDatabaseKeys(Connection conn) throws SQLException {
        Map<String, TableKeys> schemaMap = new HashMap<>();
        Set<String> allTables = getAllTableNames(conn);

        for (String table : allTables) {
            TableKeys keys = new TableKeys();

            // PKs
            String pkQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND CONSTRAINT_NAME = 'PRIMARY'";
            try (PreparedStatement stmt = conn.prepareStatement(pkQuery)) {
                stmt.setString(1, conn.getCatalog());
                stmt.setString(2, table);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    keys.primaryKeys.add(rs.getString("COLUMN_NAME"));
                }
            }

            // FKs
            String fkQuery = "SELECT COLUMN_NAME, REFERENCED_TABLE_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE " +
                    "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL";
            try (PreparedStatement stmt = conn.prepareStatement(fkQuery)) {
                stmt.setString(1, conn.getCatalog());
                stmt.setString(2, table);
                ResultSet rs = stmt.executeQuery();
                while (rs.next()) {
                    keys.foreignKeys.put(rs.getString("COLUMN_NAME"), rs.getString("REFERENCED_TABLE_NAME"));
                }
            }

            schemaMap.put(table, keys);
        }

        return schemaMap;
    }

    public static class TableKeys {
        public Set<String> primaryKeys = new HashSet<>();
        public Map<String, String> foreignKeys = new HashMap<>();

        @Override
        public String toString() {
            return "PKs: " + primaryKeys + ", FKs: " + foreignKeys;
        }
    }

}
