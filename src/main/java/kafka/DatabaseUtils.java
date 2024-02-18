package kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DatabaseUtils {

    private static Properties properties = new Properties();

    static {
        loadProperties();
    }

    private static void loadProperties() {
        try {
            // Adjust path as necessary, possibly using an environment-specific strategy
            properties.load(new FileInputStream("src/main/resources/database.properties"));
        } catch (IOException e) {
            System.out.println("Could not load database properties: " + e.getMessage());
            throw new RuntimeException("Could not load database properties", e);
        }
    }

    public static List<Map<String, Object>> getActiveAlerts() {
        String url = properties.getProperty("url");
        String user = properties.getProperty("username");
        String password = properties.getProperty("password");
        List<Map<String, Object>> alerts = new ArrayList<>();

        String query = "SELECT * FROM table_change_alert " +
                "WHERE " +
                "status = 'A' AND " +
                "source = 'KAFKA' AND " +
                "((effective_date <= ? OR effective_date IS NULL) AND " +
                "(expiration_date >= ? OR expiration_date IS NULL))";

        try (Connection conn = DriverManager.getConnection(url, user, password);
                PreparedStatement pstmt = conn.prepareStatement(query)) {

            // Current timestamp
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            pstmt.setTimestamp(1, now);
            pstmt.setTimestamp(2, now);

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnName(i), rs.getObject(i));
                    }
                    alerts.add(row);
                }
            }
        } catch (SQLException e) {
            System.out.println("Failed to connect to the database: " + e.getMessage());
            throw new RuntimeException("Failed to connect to the database", e);
        }

        return alerts;
    }

    public static void main(String[] args) {
        List<Map<String, Object>> alerts = getActiveAlerts();
        alerts.forEach(System.out::println);
    }
}
