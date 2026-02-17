package es.mblcu.videomanager.infrastructure.config;

import java.io.InputStream;
import java.util.Properties;

public class AppProperties {

    private static final String FILE_NAME = "application.properties";

    private final Properties properties;

    private AppProperties(Properties properties) {
        this.properties = properties;
    }

    public static AppProperties load() {
        Properties props = new Properties();

        try (InputStream input = AppProperties.class.getClassLoader().getResourceAsStream(FILE_NAME)) {
            if (input != null) {
                props.load(input);
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot load " + FILE_NAME, ex);
        }

        return new AppProperties(props);
    }

    public String get(String envKey, String propertyKey, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }

        String systemValue = System.getProperty(propertyKey);
        if (systemValue != null && !systemValue.isBlank()) {
            return systemValue;
        }

        String propertyValue = properties.getProperty(propertyKey);
        if (propertyValue != null && !propertyValue.isBlank()) {
            return propertyValue;
        }

        return defaultValue;
    }

    public long getLong(String envKey, String propertyKey, long defaultValue) {
        return Long.parseLong(get(envKey, propertyKey, String.valueOf(defaultValue)));
    }

}
