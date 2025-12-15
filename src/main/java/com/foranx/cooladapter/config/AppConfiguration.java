package com.foranx.cooladapter.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

public class AppConfiguration {

    private List<String> supportedExtensions = List.of("csv", "txt");
    private String logFolder;
    private String fallbackLogName;
    private String credentials = "INPUTT/123456";
    private String logLevel = "INFO";
    private String directory = "~/S_FILE_UPLOADER";
    private String activeMqUrl = "tcp://192.168.38.3:5445";
    private String queue = "java:/queue/t24DSPPACKAGERQueue";
    private static final Logger log = Logger.getLogger(AppConfiguration.class.getName());

    public AppConfiguration() {
    }

    public void init(InputStream input) {
        Properties props = new Properties();

        try {
            if (input == null) {
                throw new IllegalStateException("application.properties not found via ServletContext");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
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

    public void validate() {
        if (directory == null || !Files.isDirectory(Paths.get(directory))) {
            throw new IllegalStateException("Directory " + directory + " does not exist");
        }

        if (logFolder == null || logFolder.isBlank()) {
            throw new IllegalStateException("logFolder is not set");
        }
        Path logPath = Paths.get(logFolder);
        Path parentDir = logPath.getParent();
        if (parentDir == null || !Files.isDirectory(parentDir)) {
            throw new IllegalStateException("Log directory does not exist: " + parentDir);
        }

        if (fallbackLogName == null || fallbackLogName.isBlank()) {
            throw new IllegalStateException("Missing fallbackLogName");
        }
        Path fallbackPath = logPath.getParent().resolve(fallbackLogName);
        Path fallbackParent = fallbackPath.getParent();
        if (fallbackParent == null || !Files.isDirectory(fallbackParent)) {
            throw new IllegalStateException(
                    "Fallback log directory does not exist: " + fallbackParent
            );
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

    public void logConfiguration() {
        log.info("=== Application configuration loaded ===");
        log.info("supportedExtensions = " + supportedExtensions);
        log.info("logFolder           = " + logFolder);
        log.info("fallbackLogName     = " + fallbackLogName);
        log.info("directory           = " + directory);
        log.info("activeMqUrl         = " + activeMqUrl);
        log.info("queue               = " + queue);
        log.info("logLevel            = " + logLevel);
        log.info("credentials         = " + mask(credentials));
        log.info("========================================");
    }

    private String mask(String value) {
        if (value == null) return null;
        int idx = value.indexOf('/');
        return idx > 0 ? value.substring(0, idx) + "/******" : "******";
    }
}
