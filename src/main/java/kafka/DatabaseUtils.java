/*
 * COPYRIGHT(c) 2024 Trenova
 *
 * This file is part of Trenova.
 *
 * The Trenova software is licensed under the Business Source License 1.1. You are granted the right
 * to copy, modify, and redistribute the software, but only for non-production use or with a total
 * of less than three server instances. Starting from the Change Date (November 16, 2026), the
 * software will be made available under version 2 or later of the GNU General Public License.
 * If you use the software in violation of this license, your rights under the license will be
 * terminated automatically. The software is provided "as is," and the Licensor disclaims all
 * warranties and conditions. If you use this license's text or the "Business Source License" name
 * and trademark, you must comply with the Licensor's covenants, which include specifying the
 * Change License as the GPL Version 2.0 or a compatible license, specifying an Additional Use
 * Grant, and not modifying the license in any other way.
 */

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for database operations related to Kafka alert management.
 * It provides functionalities to retrieve active alerts from the database,
 * to get a list of Kafka topics based on these alerts, and to convert these
 * alerts to JSON format.
 */
public class DatabaseUtils {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaListener.class);
    private static Properties PROPERTIES = new Properties();

    // Static block for loading database configuration properties
    static {
        loadProperties();
    }

    /**
     * Loads database configuration properties from a properties file.
     * The properties file is expected to contain database connection details such
     * as URL, username, and password.
     */
    private static void loadProperties() {
        try (FileInputStream fis = new FileInputStream("src/main/resources/database.properties")) {
            PROPERTIES.load(fis);
        } catch (IOException ex) {
            LOG.debug("Could not load database properties: " + ex.getMessage());
            throw new RuntimeException("Could not load database properties", ex);
        }
    }

    /**
     * Retrieves active alerts from the database that are relevant to Kafka.
     * An alert is considered active if it is currently within its effective date
     * range and is marked for Kafka.
     *
     * @return A list of maps, where each map represents an active alert with its
     *         column names and values.
     */
    public static List<Map<String, Object>> getActiveAlerts() {
        String url = PROPERTIES.getProperty("url");
        String user = PROPERTIES.getProperty("username");
        String password = PROPERTIES.getProperty("password");
        List<Map<String, Object>> alerts = new ArrayList<>();

        String query = "SELECT * FROM table_change_alert " +
                "WHERE " +
                "status = 'A' AND " +
                "source = 'KAFKA' AND " +
                "((effective_date <= ? OR effective_date IS NULL) AND " +
                "(expiration_date >= ? OR expiration_date IS NULL))";

        try (Connection conn = DriverManager.getConnection(url, user, password);
                PreparedStatement pstmt = conn.prepareStatement(query)) {

            // Use the current timestamp to filter alerts based on their effective and
            // expiration dates.
            Timestamp now = Timestamp.valueOf(LocalDateTime.now());
            pstmt.setTimestamp(1, now);
            pstmt.setTimestamp(2, now);

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Iterate over the result set and populate the list of alerts.
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnName(i), rs.getObject(i));
                    }
                    alerts.add(row);
                }
            }
        } catch (SQLException e) {
            LOG.debug("Failed to connect to the database: " + e.getMessage());
            throw new RuntimeException("Failed to connect to the database", e);
        }

        return alerts;
    }

    /**
     * Extracts and returns a list of Kafka topics from the active alerts.
     * This method filters out any null or empty topic names.
     *
     * @return A list of strings where each string is a Kafka topic name.
     */
    public static List<String> getTopicList() {
        List<Map<String, Object>> alerts = getActiveAlerts();
        List<String> topics = new ArrayList<>();
        for (Map<String, Object> alert : alerts) {
            String topic = (String) alert.get("topic");
            if (topic != null && !topic.isEmpty()) {
                topics.add(topic);
            }
        }
        return topics;
    }
}
