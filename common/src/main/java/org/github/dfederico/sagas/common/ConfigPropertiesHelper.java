package org.github.dfederico.sagas.common;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class ConfigPropertiesHelper {

    public static final String KAFKA_CONFIG_PROPERTIES_PREFIX = "kafka.config.";

    public static Properties filterProperties(Properties sourceProperties, String matchPrefix) {
        Properties props = new Properties();

        sourceProperties.forEach((key, value) -> {
            if (key instanceof String && ((String) key).startsWith(matchPrefix)) {
                props.put(((String) key).replaceFirst(matchPrefix, ""), value);
            }
        });
        return props;
    }

    public static Properties loadApplicationProperties(Path propertiesPath) {
        Properties appProps = new Properties();
        try {
            appProps = readConfigFile(propertiesPath);
        } catch (Exception e) {
            System.out.println("Provide a configuration property file as argument");
            System.err.printf("Exception while configuring service %s%n", e.getMessage());
            e.printStackTrace(System.err);
            System.exit(1);
        }
        return appProps;
    }

    private static Properties readConfigFile(Path filepath) throws Exception {
        try (Reader reader = Files.newBufferedReader(filepath)) {
            Properties props = new Properties();
            props.load(reader);
            return props;
        }
    }



}
