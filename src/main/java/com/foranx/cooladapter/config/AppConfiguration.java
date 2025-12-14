package com.foranx.cooladapter.config;

import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AppConfiguration {

    private List<String> supportedExtensions = List.of("csv", "txt");
    private String logFolder;
    private String fallbackLogName = "INFO";
    private String directory = "/t24/T24/bnk/stud";
    private String activeMqUrl = "tcp://192.168.38.3:5445";
    private String queue = "java:/queue/t24DSPPACKAGERQueue";
    private String credentials;
    private String logLevel;

    public AppConfiguration() {
        loadProperties();
    }

    private void loadProperties() {
        Properties props = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                System.out.println("application.properties not found in classpath, using defaults");
                return;
            }
            props.load(input);

            if (props.getProperty("supportedExtensions") != null) {
                supportedExtensions = Arrays.asList(props.getProperty("supportedExtensions").split(","));
            }
            logFolder = props.getProperty("logFolder", logFolder);
            fallbackLogName = props.getProperty("fallbackLogName", fallbackLogName);
            directory = props.getProperty("directory", directory);
            activeMqUrl = props.getProperty("activeMqUrl", activeMqUrl);
            queue = props.getProperty("queue", queue);
            credentials = props.getProperty("credentials", credentials);
            logLevel = props.getProperty("logLevel", logLevel);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
    }

    @PostConstruct
    public void validate() {
        try {
            if (directory == null || directory.isBlank() || !Files.isDirectory(Paths.get(directory))) {
                throw new IllegalStateException(String.format("Directory %s does not exist", directory));
            }

            if (logFolder == null || logFolder.isBlank() || !Files.isDirectory(Paths.get(logFolder))) {
                throw new IllegalStateException(String.format("LogFolder %s does not exist", logFolder));
            }

            if (fallbackLogName == null || fallbackLogName.isBlank()) {
                throw new IllegalStateException("Missing required parameter: fallbackLogName");
            }

            if (activeMqUrl == null || activeMqUrl.isBlank()) {
                throw new IllegalStateException("ActiveMQ URL is required");
            }

            if (queue == null || queue.isBlank()) {
                throw new IllegalStateException("Queue name is required");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Configuration validation failed: " + e.getMessage(), e);
        }
    }

    public List<String> getSupportedExtensions() { return supportedExtensions; }
    public String getLogFolder() { return logFolder; }
    public String getFallbackLogName() { return fallbackLogName; }
    public String getCredentials() { return credentials; }
    public String getLogLevel() { return logLevel; }
    public String getDirectory() { return directory; }
    public String getActiveMqUrl() { return activeMqUrl; }
    public String getQueue() { return queue; }

    public Map<String, Object> getAll() {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put("supportedExtensions", supportedExtensions);
        map.put("logFolder", logFolder);
        map.put("fallbackLogName", fallbackLogName);
        map.put("credentials", credentials);
        map.put("logLevel", logLevel);
        map.put("directory", directory);
        map.put("activeMqUrl", activeMqUrl);
        map.put("queue", queue);
        return map;
    }
}
