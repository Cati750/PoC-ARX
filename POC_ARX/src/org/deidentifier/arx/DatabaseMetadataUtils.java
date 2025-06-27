package org.deidentifier.arx;

import java.sql.*;
import java.util.*;

//métodos para identificação automática de pk e fk
public class DatabaseMetadataUtils {

    public static Set<String> getAllTableNames(Connection conn) throws SQLException {
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

    public static Map<String, Set<String>> getColumnOccurrences(Connection conn) throws SQLException {
        Map<String, Set<String>> colunaParaTabelas = new HashMap<>();
        String query = "SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ?";

        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            stmt.setString(1, conn.getCatalog());
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                String tabela = rs.getString("TABLE_NAME").toLowerCase();
                String coluna = rs.getString("COLUMN_NAME").toLowerCase();

                colunaParaTabelas.computeIfAbsent(coluna, k -> new HashSet<>()).add(tabela);
            }
        }

        return colunaParaTabelas;
    }

}
