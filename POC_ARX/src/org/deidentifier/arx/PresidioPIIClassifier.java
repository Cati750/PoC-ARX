package org.deidentifier.arx;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.io.*;
import java.util.*;
import org.json.*;

// utilização do presidio para classificação automática dos campos (integração com ARX)
public class PresidioPIIClassifier {

    public static String getSample(Connection conn, String tabela, String coluna) throws SQLException {
        String query = "SELECT " + coluna + " FROM " + tabela + " WHERE " + coluna + " IS NOT NULL LIMIT 1";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null; // query para obter uma amostra (valor não nulo) de uma dada coluna (para o presidio conseguir classificá-la)
    }

    public static JSONArray analisarComPresidio(String texto) throws IOException {
        URL url = new URL("http://localhost:3000/analyze"); //envia o texto para o Presidio Analyzer API via POST (presidio está a correr localmente em python)
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);

        JSONObject payload = new JSONObject();
        payload.put("text", texto); // recebe uma resposta JSON com as entidades detetadas
        payload.put("language", "en");

        try (OutputStream os = con.getOutputStream()) {
            os.write(payload.toString().getBytes("utf-8"));
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
        StringBuilder resposta = new StringBuilder();
        String linha;
        while ((linha = br.readLine()) != null) {
            resposta.append(linha.trim());
        }

        return new JSONArray(resposta.toString());
    }

    public static Map<String, AttributeType> classificarTabela(String tabela) throws Exception {
        Map<String, AttributeType> mapaTipos = new HashMap<>();

        String db = "poc_arx"; // ligação à base de dados
        String user = "root";
        String password = "";

        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + db, user, password);
        ResultSet rsCols = conn.getMetaData().getColumns(null, null, tabela, null);

        while (rsCols.next()) {
            String coluna = rsCols.getString("COLUMN_NAME");
            String amostra = getSample(conn, tabela, coluna);
            if (amostra == null || amostra.isEmpty()) continue;

            JSONArray resultado = analisarComPresidio(amostra);
            Set<String> entidades = new HashSet<>();
            for (int i = 0; i < resultado.length(); i++) {
                entidades.add(resultado.getJSONObject(i).getString("entity_type"));
            } //Para cada coluna: obtém uma amostra com getSample; usa analisarComPresidio para detetar as entidades; classifica com base nas entidades detetadas

            AttributeType tipo; //automatização da classificação dos campos
            if (entidades.contains("EMAIL_ADDRESS") || entidades.contains("PHONE_NUMBER") || entidades.contains("PERSON")) {
                tipo = AttributeType.IDENTIFYING_ATTRIBUTE;
            } else if (entidades.contains("DATE_TIME") || entidades.contains("LOCATION") || entidades.contains("GENDER") || entidades.contains("FACILITY")) { // gender e facility são ambas entidades treinadas por mim
                tipo = AttributeType.QUASI_IDENTIFYING_ATTRIBUTE;
            } else if (entidades.contains("RACE") || entidades.contains("BLOOD_TYPE")) { // ambas as entidades PII foram treinada previamente para o presidio as identificar
                tipo = AttributeType.SENSITIVE_ATTRIBUTE;
            } else {
                tipo = AttributeType.INSENSITIVE_ATTRIBUTE;
            }

            mapaTipos.put(coluna, tipo);
        }

        conn.close();
        return mapaTipos;
    }

}
