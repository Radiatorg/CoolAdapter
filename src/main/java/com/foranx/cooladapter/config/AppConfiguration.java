package com.foranx.cooladapter.config;

import jakarta.annotation.PostConstruct;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AppConfiguration {
    private List<String> supportedExtensions = List.of("csv", "txt");
    private String logFolder = "INPUTT/123456";
    private String fallbackLogName = "INFO";
    private String directory = "~/";
    private String activeMqUrl = "tcp://192.168.38.3:5445";
    private String queue = "java:/queue/t24DSPPACKAGERQueue";

    private String credentials;
    private String logLevel;

    public List<String> getSupportedExtensions() {
        return supportedExtensions;
    }

    public void setSupportedExtensions(List<String> supportedExtensions) {
        this.supportedExtensions = supportedExtensions;
    }

    public String getLogFolder() {
        return logFolder;
    }

    public void setLogFolder(String logFolder) {
        this.logFolder = logFolder;
    }

    public String getFallbackLogName() {
        return fallbackLogName;
    }

    public void setFallbackLogName(String fallbackLogName) {
        this.fallbackLogName = fallbackLogName;
    }

    public String getCredentials() {
        return credentials;
    }

    public void setCredentials(String credentials) {
        this.credentials = credentials;
    }

    public String getLogLevel() {
        return logLevel;
    }

    public void setLogLevel(String logLevel) {
        this.logLevel = logLevel;
    }

    public String getDirectory() {
        return directory;
    }

    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public String getActiveMqUrl() {
        return activeMqUrl;
    }

    public void setActiveMqUrl(String activeMqUrl) {
        this.activeMqUrl = activeMqUrl;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @PostConstruct
    public void validate() {
        try {

            if (directory == null || directory.isBlank() || !Files.isDirectory(Paths.get(directory))) {
                throw new IllegalStateException(String.format("Directory %s does not exist", directory));
            }

            if (logFolder == null || logFolder.isBlank() || !Files.isDirectory(Paths.get(logFolder))) {
                throw new IllegalStateException(String.format("Directory %s does not exist", logFolder));
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