package org.deidentifier.arx;

import org.deidentifier.arx.aggregates.HierarchyBuilderDate;
import org.deidentifier.arx.aggregates.StatisticsSummary;
import org.deidentifier.arx.criteria.DistinctLDiversity;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.risk.RiskModelSampleRisks;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class ARXUtils {
    public static Connection connectToDatabase(String url, String user, String pass) throws SQLException {
        return DriverManager.getConnection(url, user, pass);
    }

    public static Data.DefaultData loadTable(Connection conn, String tableName) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName);
        ResultSetMetaData meta = rs.getMetaData();

        int columnCount = meta.getColumnCount();
        String[] columnNames = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnNames[i] = meta.getColumnName(i + 1);
        }

        Data.DefaultData data = Data.create();
        data.add(columnNames);

        while (rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                row[i] = rs.getString(i + 1);
            }
            data.add(row);
        }

        return data;
    }


   /* public static Data.DefaultData loadAndMaybePseudonymizeTable(
            Connection conn,
            String tableName,
            DatabaseMetadataUtils.TableKeys keys,
            Map<String, Pseudonymizer> pseudonymizers,
            List<String> columnsToTrackForHierarchies
    ) throws SQLException {

        PreparedStatement stmt = conn.prepareStatement("SELECT * FROM " + tableName);
        ResultSet rs = stmt.executeQuery();
        ResultSetMetaData meta = rs.getMetaData();

        int columnCount = meta.getColumnCount();
        String[] columnNames = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnNames[i] = meta.getColumnName(i + 1);
        }

        Data.DefaultData data = Data.create();
        data.add(columnNames);

        while (rs.next()) {
            String[] row = new String[columnCount];
            for (int i = 0; i < columnCount; i++) {
                String colName = columnNames[i];
                String originalValue = rs.getString(colName);

                if ((keys.primaryKeys.contains(colName) || keys.foreignKeys.containsKey(colName))
                        && pseudonymizers.containsKey(colName)) {
                    row[i] = pseudonymizers.get(colName).get(originalValue);
                } else {
                    row[i] = originalValue;

                    if (columnsToTrackForHierarchies != null && columnsToTrackForHierarchies.contains(colName)) {
                        // Podes recolher os valores únicos externamente se necessário para hierarquias personalizadas
                    }
                }
            }
            data.add(row);
        }

        return data;
    }*/

    public static Data.DefaultData applyPseudonymization(
            Data.DefaultData originalData,
            DatabaseMetadataUtils.TableKeys keys,
            Map<String, Pseudonymizer> pseudonymizers) {

        if (pseudonymizers == null || pseudonymizers.isEmpty()) return originalData;

        Data.DefaultData newData = Data.create();
        DataHandle handle = originalData.getHandle();
        int columnCount = handle.getNumColumns();
        String[] columnNames = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnNames[i] = handle.getAttributeName(i);
        }

        newData.add(columnNames);


        for (int i = 0; i < handle.getNumRows(); i++) {
            String[] row = new String[columnNames.length];
            for (int j = 0; j < columnNames.length; j++) {
                String colName = columnNames[j];
                String originalValue = handle.getValue(i, j);

                boolean isIdentifyingKey = keys.primaryKeys.contains(colName) || keys.foreignKeys.containsKey(colName);

                if (isIdentifyingKey && pseudonymizers.containsKey(colName) && originalValue != null) {
                    row[j] = pseudonymizers.get(colName).get(originalValue);
                } else {
                    row[j] = originalValue;
                }
            }
            newData.add(row);
        }
        // copy the original DataDefinition to preserve attribute types (such as SENSITIVE_ATTRIBUTE)
        DataDefinition originalDef = originalData.getDefinition();
        DataDefinition newDef = newData.getDefinition();

        for (String attr : columnNames) {
            AttributeType type = originalDef.getAttributeType(attr);
            newDef.setAttributeType(attr, type);
        }

        return newData;
    }

    /*public static Map<String, AttributeType> aplicarClassificacaoPresidio(String nomeTabela, Data.DefaultData data) throws Exception {
        Map<String, AttributeType> classificacao = PresidioPIIClassifier.classificarTabela(nomeTabela);
        DataDefinition def = data.getDefinition();

        System.out.println("Tabela " + nomeTabela);
        for (Map.Entry<String, AttributeType> entry : classificacao.entrySet()) {
            def.setAttributeType(entry.getKey(), entry.getValue());
            System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
        }

        return classificacao;
    }*/

    public static ARXConfiguration criarConfiguracaoGenerica() {
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new KAnonymity(2));
        config.setSuppressionLimit(1d);
        return config;
    }

    public static ARXConfiguration criarConfiguracaoPacientes() {
        ARXConfiguration config = ARXConfiguration.create();
        config.addPrivacyModel(new DistinctLDiversity("race", 3));
        config.addPrivacyModel(new DistinctLDiversity("blood_type", 3));
        config.setSuppressionLimit(1d);
        return config;
    }

    public static HierarchyBuilderDate criarHierarquiaDatas(String formato) {
        DataType<Date> customDateType = DataType.createDate(formato);
        return HierarchyBuilderDate.create(
                customDateType,
                HierarchyBuilderDate.Granularity.DAY_MONTH_YEAR,
                HierarchyBuilderDate.Granularity.MONTH_YEAR,
                HierarchyBuilderDate.Granularity.YEAR,
                HierarchyBuilderDate.Granularity.DECADE
        );
    }

    public static int exportarParaBaseDeDados(DataHandle handle, String sql, Connection conn) throws SQLException {
        int insertedLines = 0;

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int row = 0; row < handle.getNumRows(); row++) {
                for (int col = 0; col < handle.getNumColumns(); col++) {
                    stmt.setString(col + 1, handle.getValue(row, col));
                }
                stmt.executeUpdate();
                insertedLines++;
            }
        }

        return insertedLines;
    }

    public static void imprimirMetricas(String nomeTabela, DataHandle handle, ARXResult result) {
        System.out.println("\nMétricas de anonimização para Tabela " + nomeTabela + ":");

        Map<String, StatisticsSummary<?>> statsMap = handle.getStatistics().getSummaryStatistics(true);
        for (Map.Entry<String, StatisticsSummary<?>> entry : statsMap.entrySet()) {
            System.out.println("→ Atributo: " + entry.getKey());
            System.out.println("   - Nº distintos: " + entry.getValue().getNumberOfDistinctValuesAsString());
            if (entry.getValue().isModeAvailable()) System.out.println("   - Moda: " + entry.getValue().getModeAsString());
            if (entry.getValue().isMinAvailable()) System.out.println("   - Mínimo: " + entry.getValue().getMinAsString());
            if (entry.getValue().isMaxAvailable()) System.out.println("   - Máximo: " + entry.getValue().getMaxAsString());
            if (entry.getValue().isMedianAvailable()) System.out.println("   - Mediana: " + entry.getValue().getMedianAsString());
        }

        ARXLattice.ARXNode node = result.getGlobalOptimum();
        for (String attr : handle.getDefinition().getQuasiIdentifyingAttributes()) {
            int level = node.getGeneralization(attr);
            System.out.println("→ Atributo: " + attr + " → Nível de generalização: " + level);
        }

        RiskModelSampleRisks risks = handle.getRiskEstimator().getSampleBasedReidentificationRisk();
        System.out.println("→ Risco máximo: " + risks.getHighestRisk());
        System.out.println("→ Risco médio: " + risks.getAverageRisk());
        System.out.println("→ Risco mínimo: " + risks.getLowestRisk());
        System.out.println("→ Registos com risco máximo: " + risks.getNumRecordsAffectedByHighestRisk());
        System.out.println("→ Registos com risco mínimo: " + risks.getNumRecordsAffectedByLowestRisk());
    }

    public static List<String> extrairColuna(DataHandle handle, String nomeColuna) {
        List<String> valores = new ArrayList<>();
        int index = handle.getColumnIndexOf(nomeColuna);
        for (int i = 0; i < handle.getNumRows(); i++) {
            valores.add(handle.getValue(i, index));
        }
        return valores;
    }

}


