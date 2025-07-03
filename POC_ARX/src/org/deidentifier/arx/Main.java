package org.deidentifier.arx;

import java.sql.*;
import java.util.*;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased.Order;

public class Main {

    public static void main(String[] args) {
        try {
            Connection conn = ARXUtils.connectToDatabase("jdbc:mysql://127.0.0.1:3306/poc_arx", "root", "");

            // Extrai metadados
            Map<String, DatabaseMetadataUtils.TableKeys> metadata = DatabaseMetadataUtils.getDatabaseKeys(conn);

            // Inicializa pseudonymizers globais com coerência relacional
            Map<String, Pseudonymizer> pseudonymizerGlobal = new HashMap<>();
            Map<String, Map<String, Pseudonymizer>> pseudonymizersPorTabela = new HashMap<>();

            for (Map.Entry<String, DatabaseMetadataUtils.TableKeys> entry : metadata.entrySet()) {
                String tabela = entry.getKey();
                DatabaseMetadataUtils.TableKeys keys = entry.getValue();

                if (tabela.equalsIgnoreCase("hospitals")) continue;

                Set<String> colunas = new HashSet<>(keys.primaryKeys);
                colunas.addAll(keys.foreignKeys.keySet());
                if (tabela.equalsIgnoreCase("diagnosis")) colunas.remove("diagnosis");

                Map<String, Pseudonymizer> local = new HashMap<>();
                for (String coluna : colunas) {
                    if (coluna.equalsIgnoreCase("hospital_id") || coluna.equalsIgnoreCase("insurance_provider_id"))
                        continue;
                    pseudonymizerGlobal.putIfAbsent(coluna, new Pseudonymizer(8));
                    local.put(coluna, pseudonymizerGlobal.get(coluna));
                }

                if (!local.isEmpty()) pseudonymizersPorTabela.put(tabela, local);
            }

            // Carrega dados originais sem pseudonimização
            Data.DefaultData patientData = ARXUtils.loadAndMaybePseudonymizeTable(conn, "patients", metadata.get("patients"), new HashMap<>(), null);
            Data.DefaultData doctorsData = ARXUtils.loadAndMaybePseudonymizeTable(conn, "doctors", metadata.get("doctors"), new HashMap<>(), null);
            Data.DefaultData diagnosisData = ARXUtils.loadAndMaybePseudonymizeTable(conn, "diagnosis", metadata.get("diagnosis"), new HashMap<>(), null);

            PreparedStatement stmt = conn.prepareStatement("SELECT hospital_id, name FROM Hospitals");
            ResultSet rs = stmt.executeQuery();
            Data.DefaultData hospitalData = Data.create();
            hospitalData.add("hospital_id", "name");
            List<String> hospitalNames = new ArrayList<>();
            while (rs.next()) {
                hospitalData.add(rs.getString("hospital_id"), rs.getString("name"));
                hospitalNames.add(rs.getString("name"));
            }

            // Classificação com Presidio
            ARXUtils.aplicarClassificacaoPresidio("patients", patientData);
            ARXUtils.aplicarClassificacaoPresidio("doctors", doctorsData);
            ARXUtils.aplicarClassificacaoPresidio("diagnosis", diagnosisData);
            ARXUtils.aplicarClassificacaoPresidio("hospitals", hospitalData);

            // Aplica pseudonimização só depois da classificação
            patientData = ARXUtils.loadAndMaybePseudonymizeTable(conn, "patients", metadata.get("patients"), pseudonymizersPorTabela.get("patients"), null);
            doctorsData = ARXUtils.loadAndMaybePseudonymizeTable(conn, "doctors", metadata.get("doctors"), pseudonymizersPorTabela.get("doctors"), null);
            diagnosisData = ARXUtils.loadAndMaybePseudonymizeTable(conn, "diagnosis", metadata.get("diagnosis"), pseudonymizersPorTabela.get("diagnosis"), null);

            // Extrair valores para hierarquias personalizadas
            List<String> raceList = ARXUtils.extrairColuna(patientData.getHandle(), "race");
            List<String> genderList = ARXUtils.extrairColuna(patientData.getHandle(), "gender");
            List<String> bloodtypeList = ARXUtils.extrairColuna(patientData.getHandle(), "blood_type");

            DataDefinition def = patientData.getDefinition();
            def.setHierarchy("birth_date", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));
            def.setHierarchy("date_of_admission", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));

            CustomRaceHierarchyBuilder builderRace = new CustomRaceHierarchyBuilder();
            builderRace.prepare(raceList.toArray(new String[0]));
            def.setHierarchy("race", builderRace.build());

            CustomGenderHierarchyBuilder builderGender = new CustomGenderHierarchyBuilder();
            builderGender.prepare(genderList.toArray(new String[0]));
            def.setHierarchy("gender", builderGender.build());

            CustomBloodTypeHierarchyBuilder builderBlood = new CustomBloodTypeHierarchyBuilder();
            builderBlood.prepare(bloodtypeList.toArray(new String[0]));
            def.setHierarchy("blood_type", builderBlood.build());

            HierarchyBuilderRedactionBased<String> builderHospital =
                    HierarchyBuilderRedactionBased.create(Order.LEFT_TO_RIGHT, Order.RIGHT_TO_LEFT, '*');
            builderHospital.prepare(hospitalNames.toArray(new String[0]));
            hospitalData.getDefinition().setHierarchy("name", builderHospital.build());

            // Configurações ARX
            ARXConfiguration config = ARXUtils.criarConfiguracaoGenerica();
            ARXConfiguration configPatient = ARXUtils.criarConfiguracaoPacientes();

            // Anonimização
            ARXAnonymizer anonymizer = new ARXAnonymizer();
            ARXResult resultsPacients = anonymizer.anonymize(patientData, configPatient);
            ARXResult resultsHospital = anonymizer.anonymize(hospitalData, config);
            ARXResult resultsDiagnosis = anonymizer.anonymize(diagnosisData, config);

            // Exportação e métricas
            Connection connTarget = DriverManager.getConnection("jdbc:mysql://localhost:3306/poc_arx", "root", "");

            ARXUtils.exportarParaBaseDeDados(resultsPacients.getOutput(),
                    "INSERT INTO anonymized_patients (patient_NIF, name, birth_date, race, gender, blood_type, contact_info) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    connTarget);
            ARXUtils.imprimirMetricas("Patients", resultsPacients.getOutput(), resultsPacients);

            ARXUtils.exportarParaBaseDeDados(doctorsData.getHandle(),
                    "INSERT INTO pseudoanonymized_doctors (cedula_medica, name) VALUES (?, ?)",
                    connTarget);

            ARXUtils.exportarParaBaseDeDados(resultsHospital.getOutput(),
                    "INSERT INTO anonymized_hospital (hospital_id, name) VALUES (?, ?)",
                    connTarget);
            ARXUtils.imprimirMetricas("Hospital", resultsHospital.getOutput(), resultsHospital);

            ARXUtils.exportarParaBaseDeDados(resultsDiagnosis.getOutput(),
                    "INSERT INTO anonymized_diagnosis (diagnosis, patient_NIF, medical_condition, date_of_admission, cedula_medica, hospital_id, insurance_provider_id, billing_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    connTarget);
            ARXUtils.imprimirMetricas("Diagnosis", resultsDiagnosis.getOutput(), resultsDiagnosis);

            conn.close();
            connTarget.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

/*
public class Main {

    // Step 1 – Pseudonymize identifying primary and foreign keys to maintain referential integrity
    public static void main(String[] args) {
        try {
            Connection conn = ARXUtils.connectToDatabase("jdbc:mysql://127.0.0.1:3306/poc_arx", "root", "");


            // initialize the mapping so that the pseudonymized primary keys are propagated to the foreign keys in the diagnosis table
            Map<String, Pseudonymizer> pseudonymizerGlobal = new HashMap<>();
            Map<String, DatabaseMetadataUtils.TableKeys> metadata = DatabaseMetadataUtils.getDatabaseKeys(conn);
            Map<String, Map<String, Pseudonymizer>> pseudonymizersPorTabela = new HashMap<>();

            for (Map.Entry<String, DatabaseMetadataUtils.TableKeys> entry : metadata.entrySet()) {
                String tabela = entry.getKey();
                DatabaseMetadataUtils.TableKeys keys = entry.getValue();

                // ignores the primary key of the hospitals table, as it does not contain relevant information
                if (tabela.equalsIgnoreCase("hospitals")) continue;

                Set<String> colunasParaPseudonimizar = new HashSet<>(keys.primaryKeys);
                if (tabela.equalsIgnoreCase("diagnosis")) colunasParaPseudonimizar.remove("diagnosis");
                colunasParaPseudonimizar.addAll(keys.foreignKeys.keySet());

                Map<String, Pseudonymizer> mapa = new HashMap<>();
                for (String coluna : colunasParaPseudonimizar) {
                    if (coluna.equalsIgnoreCase("hospital_id") || coluna.equalsIgnoreCase("insurance_provider_id")) continue;

                    pseudonymizerGlobal.putIfAbsent(coluna, new Pseudonymizer(8));
                    mapa.put(coluna, pseudonymizerGlobal.get(coluna));
                }

                if (!mapa.isEmpty()) pseudonymizersPorTabela.put(tabela, mapa);
            }

            List<String> sensiveis = Arrays.asList("race", "gender", "blood_type");
            List<String> hospitalNames = new ArrayList<>();

            Data.DefaultData patientData = ARXUtils.loadAndMaybePseudonymizeTable(
                    conn, "patients", metadata.get("patients"), pseudonymizersPorTabela.get("patients"), sensiveis);
            Data.DefaultData doctorsData = ARXUtils.loadAndMaybePseudonymizeTable(
                    conn, "doctors", metadata.get("doctors"), pseudonymizersPorTabela.get("doctors"), null);
            Data.DefaultData diagnosisData = ARXUtils.loadAndMaybePseudonymizeTable(
                    conn, "diagnosis", metadata.get("diagnosis"), pseudonymizersPorTabela.get("diagnosis"), null);

            PreparedStatement stmt = conn.prepareStatement("SELECT hospital_id, name FROM Hospitals");
            ResultSet rs = stmt.executeQuery();
            Data.DefaultData hospitalData = Data.create();
            hospitalData.add("hospital_id", "name");
            while (rs.next()) {
                String id = rs.getString("hospital_id");
                String name = rs.getString("name");
                hospitalData.add(id, name);
                hospitalNames.add(name);
            }
            // step 2 - presidio classification
            ARXUtils.aplicarClassificacaoPresidio("patients", patientData);
            ARXUtils.aplicarClassificacaoPresidio("doctors", doctorsData);
            ARXUtils.aplicarClassificacaoPresidio("diagnosis", diagnosisData);
            ARXUtils.aplicarClassificacaoPresidio("hospitals", hospitalData);


            List<String> raceList = new ArrayList<>();
            List<String> genderList = new ArrayList<>();
            List<String> bloodtypeList = new ArrayList<>();

            DataHandle handle = patientData.getHandle();

            int raceIndex = handle.getColumnIndexOf("race");
            int genderIndex = handle.getColumnIndexOf("gender");
            int bloodTypeIndex = handle.getColumnIndexOf("blood_type");

            for (int i = 0; i < handle.getNumRows(); i++) {
                raceList.add(handle.getValue(i, raceIndex));
                genderList.add(handle.getValue(i, genderIndex));
                bloodtypeList.add(handle.getValue(i, bloodTypeIndex));
            }

            String[] race = raceList.toArray(new String[0]);
            String[] gender = genderList.toArray(new String[0]);
            String[] blood_type = bloodtypeList.toArray(new String[0]);


            ARXConfiguration config = ARXUtils.criarConfiguracaoGenerica();
            ARXConfiguration configPatient = ARXUtils.criarConfiguracaoPacientes();

            DataDefinition def = patientData.getDefinition();
            def.setHierarchy("birth_date", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));
            def.setHierarchy("date_of_admission", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));

            DataDefinition definition = patientData.getDefinition();

            // --------------------------------------------------------------------------------------

            // step 3 – define generalization hierarchies for sensitive fields and quasi-identifiers (custom or preconfigured)
            definition.setHierarchy("birth_date", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));

            // --------------------------------------------------------------------------------------

            definition.setHierarchy("date_of_admission", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));

            // --------------------------------------------------------------------------------------

            // import custom hierarchy construction methods for race, gender, and blood type.
            CustomRaceHierarchyBuilder builderRace = new CustomRaceHierarchyBuilder();
            System.out.println("RACE VALUES LENGTH: " + race.length);
            System.out.println("RACE VALUES: " + Arrays.toString(race));
            builderRace.prepare(race);
            Hierarchy raceHierarchy = builderRace.build();
            definition.setHierarchy("race", raceHierarchy);

            // --------------------------------------------------------------------------------------

            CustomGenderHierarchyBuilder builderGender = new CustomGenderHierarchyBuilder();
            builderGender.prepare(gender);
            Hierarchy genderHierarchy = builderGender.build();
            definition.setHierarchy("gender", genderHierarchy);

            // --------------------------------------------------------------------------------------

            CustomBloodTypeHierarchyBuilder builderBlood = new CustomBloodTypeHierarchyBuilder();
            builderBlood.prepare(blood_type);
            Hierarchy bloodHierarchy = builderBlood.build();
            definition.setHierarchy("blood_type", bloodHierarchy);

            HierarchyBuilderRedactionBased<String> builderHospital =
                    HierarchyBuilderRedactionBased.create(Order.LEFT_TO_RIGHT, Order.RIGHT_TO_LEFT, '*');
            builderHospital.prepare(hospitalNames.toArray(new String[0]));
            def.setHierarchy("name", builderHospital.build());

            ARXAnonymizer anonymizer = new ARXAnonymizer();
            ARXResult resultsPacients = anonymizer.anonymize(patientData, configPatient);
            ARXResult resultsHospital = anonymizer.anonymize(hospitalData, config);
            ARXResult resultsDiagnosis = anonymizer.anonymize(diagnosisData, config);

            Connection connTarget = DriverManager.getConnection("jdbc:mysql://localhost:3306/poc_arx", "root", "");

            String sqlPatients = "INSERT INTO anonymized_patients (patient_NIF, name, birth_date, race, gender, blood_type, contact_info) VALUES (?, ?, ?, ?, ?, ?, ?)";
            ARXUtils.exportarParaBaseDeDados(resultsPacients.getOutput(), sqlPatients, connTarget);
            ARXUtils.imprimirMetricas("Patients", resultsPacients.getOutput(), resultsPacients);

            String sqlDoctor = "INSERT INTO pseudoanonymized_doctors (cedula_medica, name) VALUES (?, ?)";
            ARXUtils.exportarParaBaseDeDados(doctorsData.getHandle(), sqlDoctor, connTarget);

            String sqlHospital = "INSERT INTO anonymized_hospital (hospital_id, name) VALUES (?, ?)";
            ARXUtils.exportarParaBaseDeDados(resultsHospital.getOutput(), sqlHospital, connTarget);
            ARXUtils.imprimirMetricas("Hospital", resultsHospital.getOutput(), resultsHospital);

            String sqlDiagnosis = "INSERT INTO anonymized_diagnosis (diagnosis, patient_NIF, medical_condition, date_of_admission, cedula_medica, hospital_id, insurance_provider_id, billing_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            ARXUtils.exportarParaBaseDeDados(resultsDiagnosis.getOutput(), sqlDiagnosis, connTarget);
            ARXUtils.imprimirMetricas("Diagnosis", resultsDiagnosis.getOutput(), resultsDiagnosis);

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
*/