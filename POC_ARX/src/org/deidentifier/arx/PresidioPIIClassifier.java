package org.deidentifier.arx;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.io.*;
import java.util.*;
import org.json.*;

// using Presidio for automated field classification integrated with ARX
public class PresidioPIIClassifier {

    private static String getSample(Connection conn, String tabela, String coluna) throws SQLException {
        String query = "SELECT " + coluna + " FROM " + tabela + " WHERE " + coluna + " IS NOT NULL LIMIT 10";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null; // query to retrieve a non-null sample value from a specific column for Presidio classification purposes
    }

    private static JSONArray analisarComPresidio(String texto) throws IOException {
        URL url = new URL("http://localhost:3000/analyze"); // sends the text to the Presidio Analyzer API via POST (Presidio is running locally in Python)
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);

        JSONObject payload = new JSONObject();
        payload.put("text", texto); // receives a JSON response with the detected entities
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

    public static Map<String, AttributeType> classifyTable(String tabela) throws Exception {
        Map<String, AttributeType> mapaTipos = new HashMap<>();

        String db = "poc_arx"; // database connection
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
            } // for each column: retrieve a sample using getSample; use analyzeWithPresidio to detect entities; classify based on the detected entities

            AttributeType tipo; // automation of field classification
            if (entidades.contains("EMAIL_ADDRESS") || entidades.contains("PHONE_NUMBER") || entidades.contains("PERSON")) {
                tipo = AttributeType.IDENTIFYING_ATTRIBUTE;
            } else if (entidades.contains("DATE_TIME") || entidades.contains("LOCATION") || entidades.contains("GENDER") || entidades.contains("FACILITY")) { // the entities "gender" and "facility" were both custom-trained by me
                tipo = AttributeType.QUASI_IDENTIFYING_ATTRIBUTE;
            } else if (entidades.contains("RACE") || entidades.contains("BLOOD_TYPE")) { // both PII entities were pre-trained to enable Presidio to identify them
                tipo = AttributeType.SENSITIVE_ATTRIBUTE;
            } else {
                tipo = AttributeType.INSENSITIVE_ATTRIBUTE;
            }

            mapaTipos.put(coluna, tipo); //mapaTipos demasiado genérico (mapa=chave_valor)
        }

        conn.close();
        return mapaTipos;
    }

}
