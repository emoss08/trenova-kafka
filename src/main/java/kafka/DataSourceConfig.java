package kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

/**
 * Configures and provides a singleton {@link DataSource} instance for database
 * connections.
 * 
 * This class uses HikariCP as the connection pool implementation to manage
 * database connections
 * efficiently. It loads database configuration properties from a properties
 * file and initializes
 * a {@link HikariDataSource} with these properties. A shutdown hook is
 * registered to close the
 * data source gracefully when the application stops.
 */
public class DataSourceConfig {
    private static volatile HikariDataSource dataSource;
    private static final Properties PROPERTIES = new Properties();
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceConfig.class);

    // Static initializer block to load properties and add a shutdown hook.
    static {
        loadProperties();
        Runtime.getRuntime().addShutdownHook(new Thread(DataSourceConfig::closeDataSource));
    }

    /**
     * Loads database configuration properties from a specified properties file.
     * 
     * The properties file is expected to be located at
     * {@code src/main/resources/database.properties}
     * and should include database connection details such as URL, username, and
     * password.
     * 
     * If the properties file cannot be loaded, this method logs an error and throws
     * a runtime exception.
     */
    private static void loadProperties() {
        try (FileInputStream fis = new FileInputStream("src/main/resources/database.properties")) {
            PROPERTIES.load(fis);
        } catch (IOException ex) {
            LOG.error("Could not load database properties", ex);
            throw new RuntimeException("Could not load database properties", ex);
        }
    }

    /**
     * Provides a singleton {@link DataSource} instance.
     * 
     * This method uses a double-checked locking pattern to initialize the
     * {@link HikariDataSource}
     * instance if it's not already created. The configuration for the data source
     * is read from the
     * previously loaded properties.
     * 
     * @return A singleton {@link DataSource} instance for database connections.
     */
    public static DataSource getDataSource() {
        if (dataSource == null) {
            synchronized (DataSourceConfig.class) {
                if (dataSource == null) {
                    HikariConfig config = new HikariConfig();
                    config.setJdbcUrl(PROPERTIES.getProperty("url"));
                    config.setUsername(PROPERTIES.getProperty("username"));
                    config.setPassword(PROPERTIES.getProperty("password"));
                    config.setMaximumPoolSize(10);
                    dataSource = new HikariDataSource(config);
                    LOG.info("DataSource has been initialized.");
                }
            }
        }
        return dataSource;
    }

    /**
     * Closes the {@link DataSource} when the application is shutting down.
     * <p>
     * This method is intended to be called automatically by a shutdown hook to
     * ensure
     * that database connections are properly closed and the connection pool is
     * shutdown
     * gracefully.
     */
    private static void closeDataSource() {
        if (dataSource != null) {
            dataSource.close();
            LOG.info("DataSource has been closed.");
        }
    }
}