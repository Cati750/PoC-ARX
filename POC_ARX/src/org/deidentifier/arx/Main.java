package org.deidentifier.arx;

import java.sql.*;
import java.util.*;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.criteria.DistinctLDiversity;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased.Order;



public class Main {

    // 1º passo- pseudoanonimizar chaves primárias e estrangeiras identificadoras (como cédula médica e NIF) para manter coerência relacional
    public static void main(String[] args) {
        try { //conexão com a base de dados
            Connection conn = ARXUtils.connectToDatabase("jdbc:mysql://127.0.0.1:3306/poc_arx", "root", "");

            // criar pseudonymizers com pseudónimos aleatórios
            Pseudonymizer doctorIdMap = new Pseudonymizer(8);
            Pseudonymizer patientIdMap = new Pseudonymizer(8);

            // --------------------------------------------------------------------------------------

            // pseudoanonimizar pacientes
            List<String> bloodtypeList = new ArrayList<>();
            List<String> raceList = new ArrayList<>();
            List<String> genderList = new ArrayList<>();
            Data.DefaultData patientData = ARXUtils.loadAndPseudonymizePatients(conn, patientIdMap, bloodtypeList, raceList, genderList);

            String[] blood_type = bloodtypeList.toArray(new String[0]);
            String[] race = raceList.toArray(new String[0]);
            String[] gender = genderList.toArray(new String[0]);

            ARXUtils.aplicarClassificacaoPresidio("Patients", patientData);

            // --------------------------------------------------------------------------------------

            // pseudonymize cedula_medica
            Data.DefaultData doctorsData = ARXUtils.loadAndPseudonymizeDoctors(conn, doctorIdMap);
            ARXUtils.aplicarClassificacaoPresidio("Doctors", doctorsData);

            // --------------------------------------------------------------------------------------

            // importação da tabela hospitais, a sua chave primária não necessita de pseudoanonimização, pois não contêm info relevante (é apenas 1,2,3 ...) mas continua a ser relevante para anonimização
            List<String> hospitalNameList = new ArrayList<>();
            Data.DefaultData hospitalData = ARXUtils.loadHospitalData(conn, hospitalNameList);

            String[] hospitalName = hospitalNameList.toArray(new String[0]);
            ARXUtils.aplicarClassificacaoPresidio("Hospitals", hospitalData);

            // --------------------------------------------------------------------------------------

            // pseudonymize diagnosis
            Data.DefaultData diagnosisData = ARXUtils.loadAndPseudonymizeDiagnosis(conn, patientIdMap, doctorIdMap);
            ARXUtils.aplicarClassificacaoPresidio("Diagnosis", diagnosisData);

            // --------------------------------------------------------------------------------------

            // 2ª passo: ARX Configuration - definição dos modelos de privacidade para campos quase-identificadores e sensíveis
            ARXConfiguration config = ARXConfiguration.create();
            ARXConfiguration configPatient = ARXConfiguration.create();
            config.addPrivacyModel(new KAnonymity(2)); // configuração geral para os campos quase-identificadores
            configPatient.addPrivacyModel(new DistinctLDiversity("race", 3)); // configuração específica do campo sensível raça
            configPatient.addPrivacyModel(new DistinctLDiversity("blood_type", 3)); // configuração específica do campo sensível tipo de sangue
            configPatient.setSuppressionLimit(1d);
            config.setSuppressionLimit(1d);

            DataDefinition definition = patientData.getDefinition();

            // --------------------------------------------------------------------------------------

            // 3º passo - definição das hierarquias de generalização dos campos sensíveis e quase-identificadores (personalizadas ou pré-configuradas)
            definition.setHierarchy("birth_date", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));

            // --------------------------------------------------------------------------------------

            definition.setHierarchy("date_of_admission", ARXUtils.criarHierarquiaDatas("yyyy-MM-dd"));

            // --------------------------------------------------------------------------------------

            // importação dos métodos de contrução de hierarquias personalizadas para raça, género e tipo de sangue
            CustomRaceHierarchyBuilder builderRace = new CustomRaceHierarchyBuilder();
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

            // --------------------------------------------------------------------------------------

            // criar o builder com configuração explícita de alinhamento e redaction (hierarquia de mascaramento pré-definida no github)
            HierarchyBuilderRedactionBased<String> builderHospital =
                    HierarchyBuilderRedactionBased.create(
                            Order.LEFT_TO_RIGHT,     // alinhamento (mantém a estrutura inicial)
                            Order.RIGHT_TO_LEFT,     // oculta do fim para o início
                            '*');                    // Caráter de redaction

            builderHospital.prepare(hospitalName);
            Hierarchy hospitalNameHierarchy = builderHospital.build();
            definition.setHierarchy("name", hospitalNameHierarchy);

            // --------------------------------------------------------------------------------------

            // 4º passo - efetuar a anonimização após definição de todas as regras
            ARXAnonymizer anonymizer = new ARXAnonymizer();
            ARXResult resultsPacients = anonymizer.anonymize(patientData, configPatient);
            ARXResult resultsHospital = anonymizer.anonymize(hospitalData, config);
            //ARXResult resultsDoctor = anonymizer.anonymize(doctorsData, config); //não necessita de anonimização uma vez que o seu ID foi identificado como insensível (pseudoanonimizado) e o seu nome como atributo identificador, logo o ARX nao tem nada para anonimizar nesta tabela
            ARXResult resultsDiagnosis = anonymizer.anonymize(diagnosisData, config);

            // --------------------------------------------------------------------------------------

            // exportação do output de anonimização para todas as tabelas da base de dados

            Connection connTarget = DriverManager.getConnection("jdbc:mysql://localhost:3306/poc_arx", "root", "");

            // --------------------------------------------------------------------------------------

            DataHandle handlePacients = resultsPacients.getOutput();

            String sqlPatients = "INSERT INTO anonymized_patients (patient_NIF, name, birth_date, race, gender, blood_type, contact_info) VALUES (?, ?, ?, ?, ?, ?, ?)";
            ARXUtils.exportarParaBaseDeDados(resultsPacients.getOutput(), sqlPatients, connTarget);

            ARXUtils.imprimirMetricas("Patients", handlePacients, resultsPacients);

            // --------------------------------------------------------------------------------------

            String sqlDoctor = "INSERT INTO pseudoanonymized_doctors (cedula_medica, name) VALUES (?, ?)";
            ARXUtils.exportarParaBaseDeDados(doctorsData.getHandle(), sqlDoctor, connTarget);

            // --------------------------------------------------------------------------------------

            DataHandle handleHospital = resultsHospital.getOutput();

            String sqlHospital = "INSERT INTO anonymized_hospital (hospital_id, name) VALUES (?, ?)";
            ARXUtils.exportarParaBaseDeDados(resultsHospital.getOutput(), sqlHospital, connTarget);

            ARXUtils.imprimirMetricas("Hospital", handleHospital, resultsHospital);

            // --------------------------------------------------------------------------------------

            DataHandle handleDiagnosis = resultsDiagnosis.getOutput();

            String sqlDiagnosis = "INSERT INTO anonymized_diagnosis (diagnosis, patient_NIF, medical_condition, date_of_admission, cedula_medica, hospital_id, insurance_provider_id, billing_amount) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            ARXUtils.exportarParaBaseDeDados(resultsDiagnosis.getOutput(), sqlDiagnosis, connTarget);

            ARXUtils.imprimirMetricas("Diagnosis", handleDiagnosis, resultsDiagnosis);
            // --------------------------------------------------------------------------------------

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}