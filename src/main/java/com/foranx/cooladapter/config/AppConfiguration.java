package com.foranx.cooladapter.config;

import jakarta.annotation.PostConstruct;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class AppConfiguration {

    private List<String> supportedExtensions = List.of("csv", "txt");
    private String logFolder;
    private String fallbackLogName = "INFO";
    private String directory = "/t24/T24/bnk/stud";
    private String activeMqUrl = "tcp://192.168.38.3:5445";
    private String queue = "java:/queue/t24DSPPACKAGERQueue";
    private String credentials = "admin/1234";
    private String logLevel = "DEBUG";

    public AppConfiguration() {
        // Конструктор пустой — загрузка происходит в @PostConstruct
    }

    public void init() {
        Properties props = new Properties();

        try (var input = AppConfiguration.class
                .getClassLoader()
                .getResourceAsStream("application.properties")) {

            if (input != null) {
                System.out.println("✅ application.properties loaded from classpath");
                props.load(input);
            } else {
                System.out.println("⚠️ application.properties not found in classpath");
            }

        } catch (IOException e) {
            throw new RuntimeException("Ошибка при загрузке application.properties", e);
        }

        supportedExtensions = Optional.ofNullable(props.getProperty("supportedExtensions"))
                .map(s -> Arrays.asList(s.split(",")))
                .orElse(supportedExtensions);

        logFolder = props.getProperty("logFolder", logFolder);
        fallbackLogName = props.getProperty("fallbackLogName", fallbackLogName);
        directory = props.getProperty("directory", directory);
        activeMqUrl = props.getProperty("activeMqUrl", activeMqUrl);
        queue = props.getProperty("queue", queue);
        credentials = props.getProperty("credentials", credentials);
        logLevel = props.getProperty("logLevel", logLevel);

        validate();
    }

    public void validate() {  // package-private
        if (directory == null || !Files.isDirectory(Paths.get(directory))) {
            throw new IllegalStateException("Directory " + directory + " does not exist");
        }
        if (logFolder == null || !Files.exists(Paths.get(logFolder)) || !Files.isRegularFile(Paths.get(logFolder))) {
            throw new IllegalStateException("Log file " + logFolder + " does not exist");
        }
        if (fallbackLogName == null || fallbackLogName.isBlank()) {
            throw new IllegalStateException("Missing fallbackLogName");
        }
        if (activeMqUrl == null || activeMqUrl.isBlank()) {
            throw new IllegalStateException("ActiveMQ URL is required");
        }
        if (queue == null || queue.isBlank()) {
            throw new IllegalStateException("Queue name is required");
        }
    }

    // Геттеры
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
