package org.deidentifier.arx;

import java.sql.*;
import java.util.*;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased.Order;


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

            List<String> hospitalNames = new ArrayList<>();

            // CLASSIFICAR E CARREGAR PATIENTS
            Map<String, AttributeType> tiposPatients = PresidioPIIClassifier.classificarTabela("patients");

            System.out.println("\nClassificação Presidio → Tabela: patients");
            for (Map.Entry<String, AttributeType> entry : tiposPatients.entrySet()) {
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            Data.DefaultData patientData = ARXUtils.loadTable(conn, "patients");
            DataDefinition defPatients = patientData.getDefinition();
            for (Map.Entry<String, AttributeType> entry : tiposPatients.entrySet()) {
                defPatients.setAttributeType(entry.getKey(), entry.getValue());
            }

            // CLASSIFICAR E CARREGAR DOCTORS
            Map<String, AttributeType> tiposDoctors = PresidioPIIClassifier.classificarTabela("doctors");

            System.out.println("\nClassificação Presidio → Tabela: doctors");
            for (Map.Entry<String, AttributeType> entry : tiposDoctors.entrySet()) {
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            Data.DefaultData doctorsData = ARXUtils.loadTable(conn, "doctors");
            DataDefinition defDoctors = doctorsData.getDefinition();
            for (Map.Entry<String, AttributeType> entry : tiposDoctors.entrySet()) {
                defDoctors.setAttributeType(entry.getKey(), entry.getValue());
            }

            // CLASSIFICAR E CARREGAR DIAGNOSIS
            Map<String, AttributeType> tiposDiagnosis = PresidioPIIClassifier.classificarTabela("diagnosis");

            System.out.println("\nClassificação Presidio → Tabela: diagnosis");
            for (Map.Entry<String, AttributeType> entry : tiposDiagnosis.entrySet()) {
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            Data.DefaultData diagnosisData = ARXUtils.loadTable(conn, "diagnosis");
            DataDefinition defDiagnosis = diagnosisData.getDefinition();
            for (Map.Entry<String, AttributeType> entry : tiposDiagnosis.entrySet()) {
                defDiagnosis.setAttributeType(entry.getKey(), entry.getValue());
            }

            // CARREGAR MANUALMENTE HOSPITALS + CLASSIFICAR
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

            Map<String, AttributeType> tiposHospitals = PresidioPIIClassifier.classificarTabela("hospitals");

            System.out.println("\nClassificação Presidio → Tabela: hospitals");
            for (Map.Entry<String, AttributeType> entry : tiposHospitals.entrySet()) {
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            DataDefinition defHospitals = hospitalData.getDefinition();
            for (Map.Entry<String, AttributeType> entry : tiposHospitals.entrySet()) {
                defHospitals.setAttributeType(entry.getKey(), entry.getValue());
            }


            patientData = ARXUtils.applyPseudonymization(patientData, metadata.get("patients"), pseudonymizersPorTabela.get("patients"));
            doctorsData = ARXUtils.applyPseudonymization(doctorsData, metadata.get("doctors"), pseudonymizersPorTabela.get("doctors"));
            diagnosisData = ARXUtils.applyPseudonymization(diagnosisData, metadata.get("diagnosis"), pseudonymizersPorTabela.get("diagnosis"));


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