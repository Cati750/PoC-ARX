package org.deidentifier.arx;

import java.sql.*;
import java.util.*;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased.Order;


public class Main {

    // Step 1 – Identify and prepare primary and foreign key columns for pseudonymization
    public static void main(String[] args) {
        try {
            Connection conn = ARXUtils.connectToDatabase("jdbc:mysql://127.0.0.1:3306/poc_arx", "root", "");

            // initialize the mapping so that the pseudonymized primary keys are propagated to the foreign keys in the diagnosis table
            Map<String, Pseudonymizer> pseudonymizerGlobal = new HashMap<>();
            Map<String, DatabaseMetadataUtils.TableKeys> metadata = DatabaseMetadataUtils.getDatabaseKeys(conn);
            Map<String, Map<String, Pseudonymizer>> pseudonymizersPerTable = new HashMap<>();

            for (Map.Entry<String, DatabaseMetadataUtils.TableKeys> entry : metadata.entrySet()) {
                String table = entry.getKey();
                DatabaseMetadataUtils.TableKeys keys = entry.getValue();

                // ignores the primary key of the hospitals table, as it does not contain relevant information
                if (table.equalsIgnoreCase("hospitals")) continue;

                Set<String> columnsToPseudonymize = new HashSet<>(keys.primaryKeys);
                if (table.equalsIgnoreCase("diagnosis")) columnsToPseudonymize.remove("diagnosis");
                columnsToPseudonymize.addAll(keys.foreignKeys.keySet());

                Map<String, Pseudonymizer> map = new HashMap<>();
                for (String column : columnsToPseudonymize) {
                    if (column.equalsIgnoreCase("hospital_id") || column.equalsIgnoreCase("insurance_provider_id")) continue;

                    pseudonymizerGlobal.putIfAbsent(column, new Pseudonymizer(8));
                    map.put(column, pseudonymizerGlobal.get(column));
                }

                if (!map.isEmpty()) pseudonymizersPerTable.put(table, map);
            }


            // Step 2 – Classify columns using Presidio and load tables for ARX anonymization
            Map<String, Data.DefaultData> tableDataMap = new HashMap<>();
            Map<String, List<String>> hospitalNameCollector = new HashMap<>(); // only used for hospitals

            String[] tables = {"patients", "doctors", "diagnosis", "hospitals"};
            for (String table : tables) {

                Map<String, AttributeType> columnTypes = PresidioPIIClassifier.classifyTable(table);

                System.out.println("\nPresidio Classification → Table: " + table);
                for (Map.Entry<String, AttributeType> entry : columnTypes.entrySet()) {
                    System.out.println("Column: " + entry.getKey() + " → ARX Type: " + entry.getValue());
                }

                Data.DefaultData data;
                if (table.equals("hospitals")) {
                    PreparedStatement stmt = conn.prepareStatement("SELECT hospital_id, name FROM Hospitals");
                    ResultSet rs = stmt.executeQuery();
                    data = Data.create();
                    data.add("hospital_id", "name");

                    List<String> names = new ArrayList<>();
                    while (rs.next()) {
                        String id = rs.getString("hospital_id");
                        String name = rs.getString("name");
                        data.add(id, name);
                        names.add(name);
                    }
                    hospitalNameCollector.put("hospitals", names);
                } else {
                    data = ARXUtils.loadTable(conn, table);
                }

                DataDefinition def = data.getDefinition();
                for (Map.Entry<String, AttributeType> entry : columnTypes.entrySet()) {
                    def.setAttributeType(entry.getKey(), entry.getValue());
                }

                tableDataMap.put(table, data);
            }
            
            // Retrieve data objects from the map
            Data.DefaultData patientData = tableDataMap.get("patients");
            Data.DefaultData doctorsData = tableDataMap.get("doctors");
            Data.DefaultData diagnosisData = tableDataMap.get("diagnosis");
            Data.DefaultData hospitalData = tableDataMap.get("hospitals");

            // Retrieve hospital names for hierarchy creation
            List<String> hospitalNames = hospitalNameCollector.getOrDefault("hospitals", new ArrayList<>());

            patientData = ARXUtils.applyPseudonymization(patientData, metadata.get("patients"), pseudonymizersPerTable.get("patients"));
            doctorsData = ARXUtils.applyPseudonymization(doctorsData, metadata.get("doctors"), pseudonymizersPerTable.get("doctors"));
            diagnosisData = ARXUtils.applyPseudonymization(diagnosisData, metadata.get("diagnosis"), pseudonymizersPerTable.get("diagnosis"));


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


            ARXConfiguration config = ARXUtils.GenericConfiguration();
            ARXConfiguration configPatient = ARXUtils.PatientConfiguration();

            DataDefinition def = patientData.getDefinition();
            def.setHierarchy("birth_date", ARXUtils.DateHierarchy("yyyy-MM-dd"));
            def.setHierarchy("date_of_admission", ARXUtils.DateHierarchy("yyyy-MM-dd"));

            DataDefinition definition = patientData.getDefinition();

            // --------------------------------------------------------------------------------------

            // step 3 – define generalization hierarchies for sensitive fields and quasi-identifiers (custom or preconfigured)
            definition.setHierarchy("birth_date", ARXUtils.DateHierarchy("yyyy-MM-dd"));

            // --------------------------------------------------------------------------------------

            definition.setHierarchy("date_of_admission", ARXUtils.DateHierarchy("yyyy-MM-dd"));

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
            ARXUtils.exportToDatabase(resultsPacients.getOutput(), sqlPatients, connTarget);
            ARXUtils.Metrics("Patients", resultsPacients.getOutput(), resultsPacients);

            String sqlDoctor = "INSERT INTO pseudoanonymized_doctors (cedula_medica, name) VALUES (?, ?)";
            ARXUtils.exportToDatabase(doctorsData.getHandle(), sqlDoctor, connTarget);

            String sqlHospital = "INSERT INTO anonymized_hospital (hospital_id, name) VALUES (?, ?)";
            ARXUtils.exportToDatabase(resultsHospital.getOutput(), sqlHospital, connTarget);
            ARXUtils.Metrics("Hospital", resultsHospital.getOutput(), resultsHospital);

            String sqlDiagnosis = "INSERT INTO anonymized_diagnosis (diagnosis, patient_NIF, medical_condition, date_of_admission, cedula_medica, hospital_id, insurance_provider_id, billing_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            ARXUtils.exportToDatabase(resultsDiagnosis.getOutput(), sqlDiagnosis, connTarget);
            ARXUtils.Metrics("Diagnosis", resultsDiagnosis.getOutput(), resultsDiagnosis);

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}