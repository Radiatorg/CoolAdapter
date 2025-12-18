package com.foranx.cooladapter.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.logging.Logger;

public class AppConfig {

    private static final Logger log = Logger.getLogger(AppConfig.class.getName());

    private List<String> supportedExtensions = new ArrayList<>(List.of("csv", "txt"));
    private String logFolder;
    private String fallbackLogName;
    private String credentials = "INPUTT/123456";
    private String logLevel = "INFO";
    private String directory = "~/S_FILE_UPLOADER";
    private String activeMqUrl = "tcp://192.168.38.3:5445";
    private String queue = "java:/queue/t24DSPPACKAGERQueue";

    private AppConfig() {}

    public static AppConfig load(InputStream input) {
        AppConfig config = new AppConfig();
        Properties props = new Properties();

        try {
            if (input == null) {
                throw new IllegalStateException("application.properties not found via ServletContext");
            }
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }

        config.applyProperties(props);
        config.validate();
        return config;
    }

    private void applyProperties(Properties props) {
        String extensions = props.getProperty("supportedExtensions");
        if (extensions != null && !extensions.isBlank()) {
            this.supportedExtensions = Arrays.asList(extensions.split(","));
        }

        this.logFolder = props.getProperty("logFolder", logFolder);
        this.fallbackLogName = props.getProperty("fallbackLogName", fallbackLogName);
        this.directory = props.getProperty("directory", directory);
        this.activeMqUrl = props.getProperty("activeMqUrl", activeMqUrl);
        this.queue = props.getProperty("queue", queue);
        this.credentials = props.getProperty("credentials", credentials);
        this.logLevel = props.getProperty("logLevel", logLevel);
    }

    private void validate() {
        if (directory == null || directory.isBlank()) {
            throw new IllegalStateException("Directory is not set");
        }
        if (logFolder == null || logFolder.isBlank()) {
            throw new IllegalStateException("logFolder is not set");
        }
        if (fallbackLogName == null || fallbackLogName.isBlank()) {
            throw new IllegalStateException("fallbackLogName is not set");
        }

        Path logPath = Paths.get(logFolder);
        if (logPath.getParent() != null && !Files.isDirectory(logPath.getParent())) {
            throw new IllegalStateException("Log directory parent does not exist: " + logPath.getParent());
        }
    }

    public void logConfiguration() {
        log.info("=== Application configuration loaded ===");
        log.info("supportedExtensions = " + supportedExtensions);
        log.info("logFolder           = " + logFolder);
        log.info("directory           = " + directory);
        log.info("activeMqUrl         = " + activeMqUrl);
        log.info("credentials         = " + mask(credentials));
        log.info("========================================");
    }

    private String mask(String value) {
        if (value == null) return null;
        int idx = value.indexOf('/');
        return idx > 0 ? value.substring(0, idx) + "/******" : "******";
    }

    // Геттеры
    public List<String> getSupportedExtensions() { return supportedExtensions; }
    public String getLogFolder() { return logFolder; }
    public String getFallbackLogName() { return fallbackLogName; }
    public String getDirectory() { return directory; }
    public String getActiveMqUrl() { return activeMqUrl; }
    public String getQueue() { return queue; }
    public String getCredentials() { return credentials; }
}