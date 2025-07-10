package org.deidentifier.arx;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.io.*;
import java.util.*;
import org.json.*;

// using Presidio for automated field classification integrated with ARX
public class PresidioPIIClassifier {

    private static String getSample(Connection conn, String table, String column) throws SQLException {
        String query = "SELECT " + column + " FROM " + table + " WHERE " + column + " IS NOT NULL LIMIT 10";
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(query)) {
            if (rs.next()) {
                return rs.getString(1);
            }
        }
        return null; // query to retrieve a non-null sample value from a specific column for Presidio classification purposes
    }

    private static JSONArray analyzeWithPresidio(String text) throws IOException {
        URL url = new URL("http://localhost:3000/analyze"); // sends the text to the Presidio Analyzer API via POST (Presidio is running locally in Python)
        HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);

        JSONObject payload = new JSONObject();
        payload.put("text", text); // receives a JSON response with the detected entities
        payload.put("language", "en");

        try (OutputStream os = con.getOutputStream()) {
            os.write(payload.toString().getBytes("utf-8"));
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"));
        StringBuilder response = new StringBuilder();
        String line;
        while ((line = br.readLine()) != null) {
            response.append(line.trim());
        }

        return new JSONArray(response.toString());
    }

    public static Map<String, AttributeType> classifyTable(String table) throws Exception {
        Map<String, AttributeType> ColumnTypeMap = new HashMap<>();

        String db = "poc_arx"; // database connection
        String user = "root";
        String password = "";

        Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/" + db, user, password);
        ResultSet rsCols = conn.getMetaData().getColumns(null, null, table, null);

        while (rsCols.next()) {
            String columnIdenfyingType = rsCols.getString("COLUMN_NAME");
            String sample = getSample(conn, table, columnIdenfyingType);
            if (sample == null || sample.isEmpty()) continue;

            JSONArray JSONresults = analyzeWithPresidio(sample);
            Set<String> entities = new HashSet<>();
            for (int i = 0; i < JSONresults.length(); i++) {
                entities.add(JSONresults.getJSONObject(i).getString("entity_type"));
            } // for each column: retrieve a sample using getSample; use analyzeWithPresidio to detect entities; classify based on the detected entities

            AttributeType type; // automation of field classification
            if (entities.contains("EMAIL_ADDRESS") || entities.contains("PHONE_NUMBER") || entities.contains("PERSON")) {
                type = AttributeType.IDENTIFYING_ATTRIBUTE;
            } else if (entities.contains("DATE_TIME") || entities.contains("LOCATION") || entities.contains("GENDER") || entities.contains("FACILITY")) { // the entities "gender" and "facility" were both custom-trained by me
                type = AttributeType.QUASI_IDENTIFYING_ATTRIBUTE;
            } else if (entities.contains("RACE") || entities.contains("BLOOD_TYPE")) { // both PII entities were pre-trained to enable Presidio to identify them
                type = AttributeType.SENSITIVE_ATTRIBUTE;
            } else {
                type = AttributeType.INSENSITIVE_ATTRIBUTE;
            }

            ColumnTypeMap.put(columnIdenfyingType, type);
        }

        conn.close();
        return ColumnTypeMap;
    }
}
