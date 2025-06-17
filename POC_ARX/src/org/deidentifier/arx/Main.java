package org.deidentifier.arx;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import org.deidentifier.arx.aggregates.HierarchyBuilderDate;
import org.deidentifier.arx.aggregates.StatisticsSummary;
import org.deidentifier.arx.criteria.KAnonymity;
import org.deidentifier.arx.criteria.DistinctLDiversity;
import java.security.SecureRandom;
import java.util.Date;
import org.deidentifier.arx.AttributeType.Hierarchy;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased;
import org.deidentifier.arx.aggregates.HierarchyBuilderRedactionBased.Order;
import org.deidentifier.arx.aggregates.HierarchyBuilderDate.Granularity;
import org.deidentifier.arx.risk.RiskEstimateBuilder;
import org.deidentifier.arx.risk.RiskModelSampleRisks;

public class Main {

    // pseudonymizer com pseudónimos aleatórios
    static class Pseudonymizer {
        private final Map<String, String> map = new HashMap<>();
        private final SecureRandom random = new SecureRandom();
        private final int length;
        private final String charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        public Pseudonymizer(int length) {
            this.length = length;
        }

        private String generateRandomId() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < length; i++) {
                sb.append(charset.charAt(random.nextInt(charset.length())));
            }
            return sb.toString();
        }

        public String get(String id) {
            return map.computeIfAbsent(id, k -> generateRandomId());
        }
    }
    // --------------------------------------------------------------------------------------

    // 1º passo- pseudoanonimizar chaves primárias e estrangeiras identificadoras (como cédula médica e NIF) para manter coerência relacional
    public static void main(String[] args) {
        try { //conexão com a base de dados
            Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/poc_arx", "root", "");

            // criar pseudonymizers com pseudónimos aleatórios
            Pseudonymizer doctorIdMap = new Pseudonymizer(8);
            Pseudonymizer pacientIdMap = new Pseudonymizer(8);

            // pseudoanonimizar pacientes
            PreparedStatement pacientsStmt = conn.prepareStatement("SELECT patient_NIF, name, birth_date, race, gender, blood_type, contact_info FROM Patients");
            ResultSet rsPacients = pacientsStmt.executeQuery();
            Data.DefaultData patientData = Data.create();
            patientData.add("patient_NIF", "name", "birth_date", "race", "gender", "blood_type", "contact_info");

            // criação de Arrays para armazenar campos sensíveis/quase-identificadores para definir hierarquias mais tarde
            List<String> bloodtypeList = new ArrayList<>();
            List<String> raceList = new ArrayList<>();
            List<String> genderList = new ArrayList<>();

            while (rsPacients.next()) {
                String patientId = String.valueOf(rsPacients.getInt("patient_NIF"));
                String name = rsPacients.getString("name");
                String birth_date = rsPacients.getString("birth_date");
                String race = rsPacients.getString("race");
                String gender = rsPacients.getString("gender");
                String blood_type = rsPacients.getString("blood_type");
                String contact_info = rsPacients.getString("contact_info");
                patientData.add(doctorIdMap.get(patientId), name, birth_date, race, gender, blood_type, contact_info);

                bloodtypeList.add(blood_type);
                raceList.add(race);
                genderList.add(gender);
            }

            String[] blood_type = bloodtypeList.toArray(new String[0]);
            String[] race = raceList.toArray(new String[0]);
            String[] gender = genderList.toArray(new String[0]);
            Map<String, AttributeType> classificacaoPatient = PresidioPIIClassifier.classificarTabela("Patients");

            DataDefinition defPatient = patientData.getDefinition();
            // para verificar se o presidio identificou cada coluna corretamente
            System.out.println("Tabela Patients");
            for (Map.Entry<String, AttributeType> entry : classificacaoPatient.entrySet()) {
                defPatient.setAttributeType(entry.getKey(), entry.getValue());
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            // --------------------------------------------------------------------------------------

            // pseudonymize cedula_medica
            PreparedStatement cedulaStmt = conn.prepareStatement("SELECT cedula_medica, name FROM Doctors");
            ResultSet rsCedula = cedulaStmt.executeQuery();
            Data.DefaultData doctorsData = Data.create();
            doctorsData.add("cedula_medica", "name");
            while (rsCedula.next()) {
                String doctorId = String.valueOf(rsCedula.getInt("cedula_medica"));
                doctorsData.add(
                        doctorIdMap.get(doctorId),
                        rsCedula.getString("name")
                );
            }
            Map<String, AttributeType> classificacaoDoctor = PresidioPIIClassifier.classificarTabela("doctors");

            DataDefinition defDoctor = doctorsData.getDefinition();
            System.out.println("Tabela Doctors");
            for (Map.Entry<String, AttributeType> entry : classificacaoDoctor.entrySet()) {
                defDoctor.setAttributeType(entry.getKey(), entry.getValue());
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            // --------------------------------------------------------------------------------------

            // pseudonymize diagnosis
            PreparedStatement diagnosisStmt = conn.prepareStatement("SELECT diagnosis, patient_NIF, medical_condition, date_of_admission, cedula_medica, hospital_id, insurance_provider_id, billing_amount FROM Diagnosis");
            ResultSet rsDiagnosis = diagnosisStmt.executeQuery();
            Data.DefaultData diagnosisData = Data.create();
            diagnosisData.add("diagnosis", "patient_NIF", "medical_condition", "date_of_admission", "cedula_medica", "hospital_id", "insurance_provider_id", "billing_amount");
            while (rsDiagnosis.next()) {
                String pacientId = String.valueOf(rsDiagnosis.getInt("patient_NIF"));
                String doctorId = String.valueOf(rsDiagnosis.getInt("cedula_medica"));
                diagnosisData.add(
                        rsDiagnosis.getString("diagnosis"),
                        pacientIdMap.get(pacientId),
                        rsDiagnosis.getString("medical_condition"),
                        rsDiagnosis.getString("date_of_admission"),
                        doctorIdMap.get(doctorId),
                        rsDiagnosis.getString("hospital_id"),
                        rsDiagnosis.getString("insurance_provider_id"),
                        rsDiagnosis.getString("billing_amount")

                );
            }
            Map<String, AttributeType> classificacaoDiagnosis = PresidioPIIClassifier.classificarTabela("Diagnosis");

            DataDefinition defDiagnosis = diagnosisData.getDefinition();
            System.out.println("Tabela Diagnosis");
            for (Map.Entry<String, AttributeType> entry : classificacaoDiagnosis.entrySet()) {
                defDiagnosis.setAttributeType(entry.getKey(), entry.getValue());
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

            // --------------------------------------------------------------------------------------

            // importação da tabela hospitais, a sua chave primária não necessita de pseudoanonimização, pois não contêm info relevante (é apenas 1,2,3 ...) mas continua a ser relevante para anonimização
            PreparedStatement hospitalStmt = conn.prepareStatement("SELECT hospital_id, name FROM Hospitals");
            ResultSet rsHospital = hospitalStmt.executeQuery();

            Data.DefaultData hospitalData = Data.create();
            hospitalData.add("hospital_id", "name");

            List<String> hospitalNameList = new ArrayList<>();

            while (rsHospital.next()) {
                String hospitalId = String.valueOf(rsHospital.getInt("hospital_id"));
                String hospitalName = rsHospital.getString("name");

                hospitalData.add(hospitalId, hospitalName);
                hospitalNameList.add(hospitalName);
            }

            String[] hospitalName = hospitalNameList.toArray(new String[0]);
            Map<String, AttributeType> classificacaoHospital = PresidioPIIClassifier.classificarTabela("Hospitals");

            DataDefinition defHospital = hospitalData.getDefinition();
            System.out.println("Tabela Hospital");
            for (Map.Entry<String, AttributeType> entry : classificacaoHospital.entrySet()) {
                defHospital.setAttributeType(entry.getKey(), entry.getValue());
                System.out.println("Coluna: " + entry.getKey() + " → Tipo ARX: " + entry.getValue());
            }

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
            DataType<Date> customDateType = DataType.createDate("yyyy-MM-dd");
            // definir a hierarquia para 'birth_date' - hierarquia personalizada de DATAS (funcionalidade oferecida pelo ARX) - disponível no github
            HierarchyBuilderDate builder = HierarchyBuilderDate.create(
                    customDateType,
                    Granularity.DAY_MONTH_YEAR, // nível mais específico - a data original
                    Granularity.MONTH_YEAR,     // nível 2 - mês e ano
                    Granularity.YEAR,           // nível 3 - só o ano
                    Granularity.DECADE          // nível 4 - só a década
            );

            definition.setHierarchy("birth_date", builder);

            // --------------------------------------------------------------------------------------

            DataType<Date> customDateTypeAdmission = DataType.createDate("yyyy-MM-dd");
            // definir a hierarquia para data de admissão (mesma lõgica)
            HierarchyBuilderDate builderAdmission = HierarchyBuilderDate.create(
                    customDateTypeAdmission,
                    Granularity.DAY_MONTH_YEAR,
                    Granularity.MONTH_YEAR,
                    Granularity.YEAR,
                    Granularity.DECADE
            );

            definition.setHierarchy("date_of_admission", builderAdmission);

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
            exportToCSV(resultsPacients.getOutput(), "patients_anon.csv");

            DataHandle handlePacients = resultsPacients.getOutput();
            Map<String, StatisticsSummary<?>> statsMap = handlePacients.getStatistics().getSummaryStatistics(true);

            // métricas para avalização da anonimização efetuada com o ARX (para cada tabela)
            System.out.println("\nMétricas de anonimização para Tabela Patients:");
            for (Map.Entry<String, StatisticsSummary<?>> entry : statsMap.entrySet()) {
                String attr = entry.getKey();
                StatisticsSummary<?> summary = entry.getValue();

                System.out.println("→ Atributo: " + attr);
                System.out.println("   - Nº de valores distintos: " + summary.getNumberOfDistinctValuesAsString());
                System.out.println("   - Moda: " + summary.getModeAsString());
                if (summary.isMinAvailable()) System.out.println("   - Mínimo: " + summary.getMinAsString());
                if (summary.isMaxAvailable()) System.out.println("   - Máximo: " + summary.getMaxAsString());
                if (summary.isMedianAvailable()) System.out.println("   - Mediana: " + summary.getMedianAsString());
                System.out.println();
            }

            ARXLattice.ARXNode optimalNode = resultsPacients.getGlobalOptimum();
            for (String attr : handlePacients.getDefinition().getQuasiIdentifyingAttributes()) {
                int level = optimalNode.getGeneralization(attr);
                System.out.println("Atributo: " + attr + " → Nível de generalização: " + level);
            }

            RiskEstimateBuilder builderPacient = resultsPacients.getOutput().getRiskEstimator();

            RiskModelSampleRisks sampleRisks = builderPacient.getSampleBasedReidentificationRisk();
            System.out.println("→ Risco máximo: " + sampleRisks.getHighestRisk());
            System.out.println("→ Risco médio: " + sampleRisks.getAverageRisk());
            System.out.println("→ Risco mínimo: " + sampleRisks.getLowestRisk());
            System.out.println("→ Registos afetados pelo risco máximo: " + sampleRisks.getNumRecordsAffectedByHighestRisk());
            System.out.println("→ Registos afetados pelo risco mínimo: " + sampleRisks.getNumRecordsAffectedByHighestRisk());

            // --------------------------------------------------------------------------------------
            exportToCSV(resultsDiagnosis.getOutput(), "diagnosis_anon.csv");

            DataHandle handleDiagnosis = resultsDiagnosis.getOutput();
            Map<String, StatisticsSummary<?>> statsMap2 = handleDiagnosis.getStatistics().getSummaryStatistics(true);

            System.out.println("\nMétricas de anonimização para Tabela Diagnosis:");
            for (Map.Entry<String, StatisticsSummary<?>> entry : statsMap2.entrySet()) {
                String attr = entry.getKey();
                StatisticsSummary<?> summary = entry.getValue();

                System.out.println("→ Atributo: " + attr);
                System.out.println("   - Nº de valores distintos: " + summary.getNumberOfDistinctValuesAsString());
                System.out.println("   - Moda: " + summary.getModeAsString());
                if (summary.isMinAvailable()) System.out.println("   - Mínimo: " + summary.getMinAsString());
                if (summary.isMaxAvailable()) System.out.println("   - Máximo: " + summary.getMaxAsString());
                if (summary.isMedianAvailable()) System.out.println("   - Mediana: " + summary.getMedianAsString());
                System.out.println();
            }

            ARXLattice.ARXNode optimalNode2 = resultsDiagnosis.getGlobalOptimum();
            for (String attr : handleDiagnosis.getDefinition().getQuasiIdentifyingAttributes()) {
                int level = optimalNode2.getGeneralization(attr);
                System.out.println("Atributo: " + attr + " → Nível de generalização: " + level);
            }

            RiskEstimateBuilder builderDiagnosis = resultsDiagnosis.getOutput().getRiskEstimator();

            RiskModelSampleRisks sampleRisks2 = builderDiagnosis.getSampleBasedReidentificationRisk();
            System.out.println("→ Risco máximo: " + sampleRisks2.getHighestRisk());
            System.out.println("→ Risco médio: " + sampleRisks2.getAverageRisk());
            System.out.println("→ Risco mínimo: " + sampleRisks2.getLowestRisk());
            System.out.println("→ Registos afetados pelo risco máximo: " + sampleRisks2.getNumRecordsAffectedByHighestRisk());
            System.out.println("→ Registos afetados pelo risco mínimo: " + sampleRisks2.getNumRecordsAffectedByHighestRisk());

            // --------------------------------------------------------------------------------------

            exportToCSV(resultsHospital.getOutput(), "hospital_anon.csv");

            DataHandle handleHospital = resultsHospital.getOutput();
            Map<String, StatisticsSummary<?>> statsMap3 = handleHospital.getStatistics().getSummaryStatistics(true);

            System.out.println("\nMétricas de anonimização para Tabela Hospital:");
            for (Map.Entry<String, StatisticsSummary<?>> entry : statsMap3.entrySet()) {
                String attr = entry.getKey();
                StatisticsSummary<?> summary = entry.getValue();

                System.out.println("→ Atributo: " + attr);
                System.out.println("   - Nº de valores distintos: " + summary.getNumberOfDistinctValuesAsString());
                System.out.println("   - Moda: " + summary.getModeAsString());
                if (summary.isMinAvailable()) System.out.println("   - Mínimo: " + summary.getMinAsString());
                if (summary.isMaxAvailable()) System.out.println("   - Máximo: " + summary.getMaxAsString());
                if (summary.isMedianAvailable()) System.out.println("   - Mediana: " + summary.getMedianAsString());
                System.out.println();
            }

            ARXLattice.ARXNode optimalNode3 = resultsHospital.getGlobalOptimum();
            for (String attr : handleHospital.getDefinition().getQuasiIdentifyingAttributes()) {
                int level = optimalNode3.getGeneralization(attr);
                System.out.println("Atributo: " + attr + " → Nível de generalização: " + level);
            }

            RiskEstimateBuilder builderHosp = resultsHospital.getOutput().getRiskEstimator();

            RiskModelSampleRisks sampleRisks3 = builderHosp.getSampleBasedReidentificationRisk();
            System.out.println("→ Risco máximo: " + sampleRisks3.getHighestRisk());
            System.out.println("→ Risco médio: " + sampleRisks3.getAverageRisk());
            System.out.println("→ Risco mínimo: " + sampleRisks3.getLowestRisk());
            System.out.println("→ Registos afetados pelo risco máximo: " + sampleRisks3.getNumRecordsAffectedByHighestRisk());
            System.out.println("→ Registos afetados pelo risco mínimo: " + sampleRisks3.getNumRecordsAffectedByHighestRisk());
            exportToCSV(doctorsData.getHandle(), "doctors_pseudoanon.csv");

            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // --------------------------------------------------------------------------------------

    // função para exportar os dados para CSV
    public static void exportToCSV(DataHandle handle, String fileName) throws IOException {
        FileWriter writer = new FileWriter(fileName);

        // escreve cabeçalhos
        for (int j = 0; j < handle.getNumColumns(); j++) {
            writer.append(handle.getAttributeName(j));
            if (j < handle.getNumColumns() - 1) writer.append(",");
        }
        writer.append("\n");

        // escreve linhas de dados
        for (int i = 0; i < handle.getNumRows(); i++) {
            for (int j = 0; j < handle.getNumColumns(); j++) {
                writer.append(handle.getValue(i, j));
                if (j < handle.getNumColumns() - 1) writer.append(",");
            }
            writer.append("\n");
        }

        writer.flush();
        writer.close();
    }

}