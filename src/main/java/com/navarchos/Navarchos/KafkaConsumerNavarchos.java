package com.navarchos.Navarchos;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;
import java.util.*;
import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaConsumerNavarchos {
    @Autowired
    private SimpMessagingTemplate messagingTemplate;
    private JSONObject eventJson;
    String url = "jdbc:postgresql://";
    String user = "user";
    String password = "password";
    int correlationWindow = 300;
    static int correlationStep = 100;

    /**
     * Computes and saves to the database the correlation for a given device ID at a specific timestamp.
     *
     * @param conn      The database connection to be used for executing the necessary queries.
     * @param device_id The ID of the device for which the correlation is to be calculated.
     * @param dt        The timestamp associated with the data for which the correlation is to be calculated.
     * @throws SQLException If a database access error occurs or if the query fails.
     */
    public void findCorrelation(Connection conn, int device_id, String dt) throws SQLException {
        // Retrieve data from the database
        ArrayList<Double> mapIntake = getColumnData(conn, "mapIntake", device_id);
        ArrayList<Double> coolantTemp = getColumnData(conn, "coolantTemp", device_id);
        ArrayList<Double> MAFairFlowRate = getColumnData(conn, "MAFairFlowRate", device_id);
        ArrayList<Double> obdSpeed = getColumnData(conn, "obdSpeed", device_id);
        ArrayList<Double> intakeTemp = getColumnData(conn, "intakeTemp", device_id);
        ArrayList<Double> rpm = getColumnData(conn, "rpm", device_id);

        // Calculate correlation between pairs of columns
        double[] correlationValues = {
                calculateCorrelation(mapIntake, coolantTemp),
                calculateCorrelation(mapIntake, MAFairFlowRate),
                calculateCorrelation(mapIntake, obdSpeed),
                calculateCorrelation(mapIntake, intakeTemp),
                calculateCorrelation(mapIntake, rpm),
                calculateCorrelation(coolantTemp, MAFairFlowRate),
                calculateCorrelation(coolantTemp, obdSpeed),
                calculateCorrelation(coolantTemp, intakeTemp),
                calculateCorrelation(coolantTemp, rpm),
                calculateCorrelation(MAFairFlowRate, obdSpeed),
                calculateCorrelation(MAFairFlowRate, intakeTemp),
                calculateCorrelation(MAFairFlowRate, rpm),
                calculateCorrelation(obdSpeed, intakeTemp),
                calculateCorrelation(obdSpeed, rpm),
                calculateCorrelation(intakeTemp, rpm)};

        String correlation_query = "INSERT INTO pids_correlation(id, dt, correlation)"
                + "VALUES(?,?,?)";
        try (PreparedStatement preparedStatement = conn.prepareStatement(correlation_query)) {
            //Prepare query to insert correlation data
            preparedStatement.setInt(1, device_id);
            preparedStatement.setTimestamp(2, Timestamp.valueOf(dt));
            Array correlationArray = conn.createArrayOf("float8", Arrays.stream(correlationValues).boxed().toArray(Double[]::new));
            preparedStatement.setArray(3, correlationArray);
            preparedStatement.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        PdM(conn,dt, correlationValues, device_id);
    }


    /**
     * Retrieves data from a specific column in the buffer table for a given device ID.
     *
     * @param conn       The database connection to be used for executing the query.
     * @param columnName The name of the column from which data should be retrieved.
     * @param device_id  The ID of the device for which the data should be retrieved.
     * @return           An ArrayList of Double values containing the data from the specified column.
     * @throws SQLException If a database access error occurs or the query fails.
     */
    public static ArrayList<Double> getColumnData(Connection conn, String columnName, int device_id) throws SQLException {
        ArrayList<Double> data = new ArrayList<>();
        String query = "SELECT " + columnName + " FROM pids_buffer WHERE device_id = " + device_id;
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(query)) {
            while (rs.next()) {
                data.add(rs.getDouble(columnName));
            }
        }
        return data;
    }


    /**
     * Calculates the Pearson correlation coefficient between two datasets.
     *
     * @param x An ArrayList of Double values representing the first dataset.
     * @param y An ArrayList of Double values representing the second dataset.
     * @return  A double value representing the Pearson correlation coefficient between the two datasets.
     *          The value ranges from -1 (perfect negative correlation) to 1 (perfect positive correlation),
     *          with 0 indicating no correlation.
     * @throws IllegalArgumentException if the input lists x and y are not of the same size or if they are empty.
     */
    public static double calculateCorrelation(ArrayList<Double> x, ArrayList<Double> y) {
        if (x.size() != y.size()) {
            throw new IllegalArgumentException("Input lists must have the same size");
        }
        int n = x.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumXX = 0, sumYY = 0;
        for (int i = 0; i < n; i++) {
            double xi = x.get(i);
            double yi = y.get(i);

            sumX += xi;  // sum of elements of array X
            sumY += yi;  // sum of elements of array Y
            sumXY += xi * yi;   // sum of X[i] * Y[i]
            sumXX += xi * xi;   // sum of square of array X elements
            sumYY += yi * yi;   // sum of square of array Y elements
        }
        double numerator = n * sumXY - sumX * sumY;
        double denominator = Math.sqrt((n * sumXX - sumX * sumX) * (n * sumYY - sumY * sumY));
        // Handle division by zero (if denominator is very close to zero)
        if (Math.abs(denominator) < 1e-10) {
            return 0; // No correlation
        }
        return numerator / denominator;
    }

    /**
     * Sends data to an PdM API for a given device ID and saves the results to the database.
     *
     * @param conn              The database connection to be used for executing the query.
     * @param dt                The timestamp associated with the correlation data.
     * @param correlationValues An array of double values representing the correlation data to be sent to the API.
     * @param device_id         The ID of the device for which the PdM data is being processed.
     * @throws SQLException     If a database access error occurs or if any related query fails.
     */
    public static void PdM(Connection conn,String dt, double[] correlationValues, int device_id) {
        try {
            // Create HttpClient instance
            CloseableHttpClient httpClient = HttpClients.createDefault();

            // Create ObjectMapper instance for JSON serialization/deserialization
            ObjectMapper objectMapper = new ObjectMapper();

            // Create the request body
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("timestamp", dt);
            requestBody.put("features", correlationValues);
            requestBody.put("source", String.valueOf(device_id));

            // Convert the map to JSON
            String jsonRequestBody = objectMapper.writeValueAsString(requestBody);

            // Print the JSON string representing the request body
            System.out.println(jsonRequestBody);

            // Create HttpPost instance with endpoint URL
            HttpPost httpPost = new HttpPost("url/data");

            // Set headers
            httpPost.setHeader("Content-Type", "application/json");

            // Set request body
            httpPost.setEntity(new StringEntity(jsonRequestBody));

            // Send the POST request and get the response
            HttpResponse response = httpClient.execute(httpPost);

            //verify the valid error code first
            int statusCode = response.getCode();
            System.out.println(statusCode);
            if(statusCode==200){
                // Read and parse the response body
                ResponseBody responseBody = objectMapper.readValue(((CloseableHttpResponse) response).getEntity().getContent(), ResponseBody.class);

                // Print the response
                System.out.println("Response:");
                System.out.println("Alarm: " + responseBody.isAlarm());
                System.out.println("Description: " + responseBody.getDescription());
                System.out.println("Scores: " + responseBody.getScores());
                System.out.println("Source: " + responseBody.getSource());
                System.out.println("Thresholds: " + responseBody.getThresholds());
                System.out.println("Timestamp: " + responseBody.getTimestamp());
                String correlation_query = "INSERT INTO pids_pdm(id, dt, alarm, scores, thresholds, description) VALUES (?, ?, ?, ?, ?, ?)";
                try (PreparedStatement preparedStatement = conn.prepareStatement(correlation_query)) {
                    preparedStatement.setInt(1, Integer.parseInt(responseBody.getSource()));
                    preparedStatement.setTimestamp(2, Timestamp.valueOf(responseBody.getTimestamp()));
                    preparedStatement.setBoolean(3, responseBody.isAlarm());
                    Array scores = conn.createArrayOf("int", responseBody.getScores().toArray());
                    preparedStatement.setArray(4, scores);
                    Array thresholds = conn.createArrayOf("int", responseBody.getThresholds().toArray());
                    preparedStatement.setArray(5, thresholds);
                    preparedStatement.setString(6, responseBody.getDescription());
                    preparedStatement.execute();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            else{
                System.out.println("Error: "+statusCode);
            }
            // Close HttpClient
            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    /**
     * Sends a reset event to the PdM API for a given device.
     *
     * @param dt        The timestamp associated with the reset event.
     * @param device_id The ID of the device for which the reset event is being sent.
     */
    public static void PdMReset(String dt, int device_id) {
        try {
            // Create HttpClient instance
            CloseableHttpClient httpClient = HttpClients.createDefault();

            // Create ObjectMapper instance for JSON serialization/deserialization
            ObjectMapper objectMapper = new ObjectMapper();

            // Create the request body
            Map<String, Object> requestBody = new HashMap<>();
            requestBody.put("timestamp", dt);
            requestBody.put("description", "reset");
            requestBody.put("source", String.valueOf(device_id));

            // Convert the map to JSON
            String jsonRequestBody = objectMapper.writeValueAsString(requestBody);

            // Print the JSON string representing the request body
            System.out.println(jsonRequestBody);

            // Create HttpPost instance with endpoint URL
            HttpPost httpPost = new HttpPost("http://localhost:9000/event");

            // Set headers
            httpPost.setHeader("Content-Type", "application/json");

            // Set request body
            httpPost.setEntity(new StringEntity(jsonRequestBody));

            // Send the POST request and get the response
            HttpResponse response = httpClient.execute(httpPost);

            //verify the valid error code first
            int statusCode = response.getCode();
            System.out.println(statusCode);
            if(statusCode==200){
                System.out.println("Reset OK");
            }
            else{
                System.out.println("Error: "+statusCode);
            }
            // Close HttpClient
            httpClient.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Deletes a specified number of entries from the `pids_buffer` table for a given device ID.
     *
     * @param conn      The database connection to be used for executing the delete operation.
     * @param device_id The ID of the device whose entries should be deleted.
     * @throws SQLException If a database access error occurs or the query fails.
     */
    private static void deleteEntries(Connection conn, int device_id) throws SQLException {
        String deleteQuery = "DELETE FROM pids_buffer\n" +
                "WHERE id IN (\n" +
                "    SELECT id\n" +
                "    FROM pids_buffer\n" +
                "    WHERE device_id = ?\n" +
                "    ORDER BY created_dt\n" +
                "    LIMIT ?\n" +
                ");\n";

        try (PreparedStatement deleteStmt = conn.prepareStatement(deleteQuery)) {
            deleteStmt.setInt(1, device_id);
            deleteStmt.setInt(2, correlationStep);
            deleteStmt.executeUpdate();
            System.out.println("Deleted " + correlationStep);
        }
    }

    /**
     * Converts a date string into a Unix timestamp (seconds since the Unix epoch).
     *
     * @param dateString The date string to be converted, expected in the format "yyyy-MM-dd HH:mm:ss".
     * @return           The Unix timestamp as an integer, or 0 if the date string cannot be parsed.
     */
    public int dateToUnixTimpestamp(String dateString) {
        // Define the date format for parsing the timestamp
        //SimpleDateFormat dateFormat=new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy");
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC")); // Set the expected time zone
        try {
            // Parse the date string into a Date object
            java.util.Date date = dateFormat.parse(dateString);
            // Convert the Date object to a Unix timestamp (seconds since Unix epoch)
            long unixTimestamp = date.getTime() / 1000; // Convert milliseconds to seconds
            return (int) unixTimestamp;
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Retrieves an Integer value from a JSON object, or null if the key is not present or the value is null.
     *
     * @param json The JSON object from which to retrieve the value.
     * @param key  The key whose associated value is to be retrieved.
     * @return     The Integer value associated with the key, or null if the key is missing or the value is null.
     * @throws JSONException If there is an error parsing the JSON object.
     */
    public Integer getIntOrNull(JSONObject json, String key) throws JSONException {
        if (json.has(key) && !json.isNull(key)) {
            return json.getInt(key);
        } else {
            return null;
        }
    }

    /**
     * Sets a double value in a prepared statement, or sets the value to NULL if the key is not present or the value is null in the JSON object.
     *
     * @param stmt           The PreparedStatement in which to set the value.
     * @param parameterIndex The index of the parameter to set.
     * @param json           The JSON object from which to retrieve the value.
     * @param key            The key whose associated value is to be set in the prepared statement.
     * @throws SQLException  If a database access error occurs.
     * @throws JSONException If there is an error parsing the JSON object.
     */
    public void setDoubleOrNull(PreparedStatement stmt, int parameterIndex, JSONObject json, String key) throws SQLException, JSONException {
        if (json.has(key) && !json.isNull(key)) {
            stmt.setDouble(parameterIndex, json.getDouble(key));
        } else {
            stmt.setNull(parameterIndex, Types.DOUBLE);
        }
    }

    /**
     * Retrieves a Double value from a JSON object, or null if the key is not present or the value is null.
     *
     * @param json The JSON object from which to retrieve the value.
     * @param key  The key whose associated value is to be retrieved.
     * @return     The Double value associated with the key, or null if the key is missing or the value is null.
     * @throws JSONException If there is an error parsing the JSON object.
     */
    public Double getDoubleOrNull(JSONObject json, String key) throws JSONException {
        if (json.has(key) && !json.isNull(key)) {
            return json.getDouble(key);
        } else {
            return null;
        }
    }

    /**
     * Sets a JSON-formatted string in a prepared statement, or sets the value to NULL if the key is not present or the value is null in the JSON object.
     *
     * @param stmt           The PreparedStatement in which to set the value.
     * @param parameterIndex The index of the parameter to set.
     * @param json           The JSON object from which to retrieve the value.
     * @param key            The key whose associated value is to be set in the prepared statement.
     * @throws SQLException  If a database access error occurs.
     * @throws JSONException If there is an error parsing the JSON object.
     */
    public void setJSONOrEmpty(PreparedStatement stmt, int parameterIndex, JSONObject json, String key) throws SQLException, JSONException {

        if (json.has(key) && !json.isNull(key)) {
            String jsonString = json.get(key).toString()
                    .replaceAll("(\\w+)=(\\w+|\\{[^}]*\\}|\\[[^\\]]*\\])", "\"$1\":\"$2\"")
                    .replaceAll(":\"(\\d+(\\.\\d+)*)\"", ":$1")
                    .replaceAll(":\"\\[([\\d,]+)\\]\"", ":[$1]");
            stmt.setObject(parameterIndex, jsonString);
        } else {
            stmt.setNull(parameterIndex, Types.OTHER);
        }
    }

    /**
     * Kafka listener method that consumes messages from the "PID" topic and processes them.
     * The method decodes the message, performs checks, inserts data into tables, calculates correlations,
     * and manages buffer entries.
     *
     * @param message The message consumed from the Kafka topic in JSON format.
     * @throws Exception If any error occurs during message processing, including database operations or JSON parsing.
     */
    @KafkaListener(topics = "PID", groupId = "traccar")
    private void consumePidMessage(String message) throws Exception {
        eventJson = new JSONObject(message);

        PreparedStatement preparedStmt = null;
        //Connect to database
        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            //Retrieve driver, vehicle and group id for a given device id
            String query_dvg = "SELECT driver_id, vehicle_id, group_id FROM mgt_dvg WHERE device_id=? AND status = 1 AND disabled_dt IS NULL ";
            try (PreparedStatement pstmt = conn.prepareStatement(query_dvg)) {
                int device_id = eventJson.getInt("deviceId");
                pstmt.setInt(1, device_id);
                int driverId = -1;
                int vehicleId = -1;
                int groupId = -1;
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        // Retrieve values from each column
                        driverId = rs.getInt("driver_id");
                        vehicleId = rs.getInt("vehicle_id");
                        groupId = rs.getInt("group_id");
                    }
                    System.out.println("Vehicle ID: " + vehicleId + " Message: " + message);
                    //Prepare query to insert pid data
                    String query = "INSERT INTO data_pids_er(dt, device_id, driver_id, vehicle_id, group_id, position, pid_type, data)"
                            + "VALUES(?,?,?,?,?,POINT(?,?),?,?::jsonb)";
                    preparedStmt = conn.prepareStatement(query);
                    preparedStmt.setInt(1, dateToUnixTimpestamp(eventJson.getString("deviceTime")));
                    preparedStmt.setInt(2, device_id);
                    preparedStmt.setInt(3, driverId);
                    preparedStmt.setInt(4, vehicleId);
                    preparedStmt.setInt(5, groupId);
                    setDoubleOrNull(preparedStmt, 6, eventJson, "latitude");
                    setDoubleOrNull(preparedStmt, 7, eventJson, "longitude");
                    preparedStmt.setString(8, eventJson.getString("topic"));
                    setJSONOrEmpty(preparedStmt, 9, eventJson, "PIDAttributes");
                    preparedStmt.execute();

                    if (eventJson.has("PIDAttributes")) {
                        //Transform attributes to json format
                        String jsonString = eventJson.get("PIDAttributes").toString()
                                .replaceAll("(\\w+)=(\\w+|\\{[^}]*\\}|\\[[^\\]]*\\])", "\"$1\":\"$2\"")
                                .replaceAll(":\"(\\d+(\\.\\d+)*)\"", ":$1")
                                .replaceAll(":\"\\[([\\d,]+)\\]\"", ":[$1]");
                        try {
                            //Prepare query to insert buffer data
                            JSONObject json = new JSONObject(jsonString);
                            String buffer_query = "INSERT INTO pids_buffer(dt, device_id, rpm, obdSpeed, mapIntake, intakeTemp, coolantTemp, MAFairFlowRate)"
                                    + "VALUES(?,?,?,?,?,?,?,?)";

                            Integer rpm = getIntOrNull(json, "rpm");
                            Integer obdSpeed = getIntOrNull(json, "obdSpeed");
                            Integer mapIntake = getIntOrNull(json, "mapIntake");
                            Integer intakeTemp = getIntOrNull(json, "intakeTemp");
                            Integer coolantTemp = getIntOrNull(json, "coolantTemp");
                            Double MAFairFlowRate = getDoubleOrNull(json, "MAFairFlowRate");
                            if (rpm != null && obdSpeed != null && mapIntake != null && intakeTemp != null && coolantTemp != null && MAFairFlowRate != null) {
                                if (obdSpeed > 20 && mapIntake > 95 && intakeTemp > 1 && intakeTemp < 210 && coolantTemp > 1
                                        && coolantTemp < 210 && rpm > 1 && MAFairFlowRate > 1) {
                                    // All conditions are met
                                    preparedStmt = conn.prepareStatement(buffer_query);
                                    preparedStmt.setInt(1, dateToUnixTimpestamp(eventJson.getString("deviceTime")));
                                    preparedStmt.setInt(2, device_id);
                                    preparedStmt.setInt(3, rpm);
                                    preparedStmt.setInt(4, obdSpeed);
                                    preparedStmt.setInt(5, mapIntake);
                                    preparedStmt.setInt(6, intakeTemp);
                                    preparedStmt.setInt(7, coolantTemp);
                                    preparedStmt.setDouble(8, MAFairFlowRate);
                                    preparedStmt.execute();
                                    Statement st = conn.createStatement();
                                    //Count buffer entries for a given device id
                                    ResultSet rs_count = st.executeQuery("SELECT count(*) FROM pids_buffer where device_id=" + device_id);

                                    if (rs_count.next()) {
                                        int count = rs_count.getInt(1);
                                        if (count==1){
                                            PdMReset(eventJson.getString("dt"),device_id);
                                        }
                                        //If the entries count is the same as the window
                                        if (count == correlationWindow) {
                                            //Calculate correlation
                                            findCorrelation(conn, device_id, eventJson.getString("deviceTime"));
                                        }
                                        //If the entries count is more than the window
                                        else if (count > correlationWindow) {
                                            //If the difference from the correlation window is the same as the window step
                                            if (count - correlationStep == correlationWindow) {
                                                //Delete older entries
                                                deleteEntries(conn, device_id);
                                                //Calculate correlation
                                                findCorrelation(conn, device_id, eventJson.getString("deviceTime"));
                                            }
                                        }
                                        System.out.println("Count: " + count);
                                    } else {
                                        System.out.println("No rows returned from the query.");
                                    }
                                } else {
                                    // Conditions are not met
                                    System.out.println("One or more conditions are not met.");
                                }
                            } else {
                                // One or more values are null
                                System.out.println("One or more values are null.");
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            } catch (SQLException e) {
                e.printStackTrace();
                // Handle exceptions
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
