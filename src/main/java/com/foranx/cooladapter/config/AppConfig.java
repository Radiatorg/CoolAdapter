package com.foranx.cooladapter.config;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public record AppConfig(
        List<String> supportedExtensions,
        String logFolder,
        String fallbackLogName,
        Credentials credentials,
        Level logLevel,
        Path directory,
        URI activeMqUrl,
        String queue,
        boolean checkHashBeforeCopy
) {
    private static final Logger log = Logger.getLogger(AppConfig.class.getName());

<<<<<<< HEAD
    private List<String> supportedExtensions = new ArrayList<>(List.of("csv", "txt"));
    private String logFolder;
    private String fallbackLogName;
    private String credentials = "INPUTT/123456";
    private String logLevel = "INFO";
    private String directory = "~/S_FILE_UPLOADER";
    private String activeMqUrl = "tcp://192.168.38.3:5445";
    private String queue = "java:/queue/t24DSPPACKAGERQueue";
    private boolean checkHashBeforeCopy = true;

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
        String checkHash = props.getProperty("checkHashBeforeCopy");
        if (checkHash != null) {
            this.checkHashBeforeCopy = Boolean.parseBoolean(checkHash);
        }
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

        /*
        Path logPath = Paths.get(logFolder);
        if (logPath.getParent() != null && !Files.isDirectory(logPath.getParent())) {
            throw new IllegalStateException("Log directory parent does not exist: " + logPath.getParent());
        }

         */
    }

=======
>>>>>>> ca2b6794429a0d7a23ab3c1fd123d6a23daa40f5
    public void logConfiguration() {
        log.info(() -> """
        === Application configuration loaded ===
        supportedExtensions = %s
        logFolder           = %s
        directory           = %s
        activeMqUrl         = %s
        credentials         = %s
        checkHashBeforeCopy = %s
        ========================================
        """.formatted(
                supportedExtensions,
                logFolder,
                directory,
                activeMqUrl,
                credentials,
                checkHashBeforeCopy
        ));
    }
<<<<<<< HEAD

    private String mask(String value) {
        if (value == null) return null;
        int idx = value.indexOf('/');
        return idx > 0 ? value.substring(0, idx) + "/******" : "******";
    }

    public List<String> getSupportedExtensions() { return supportedExtensions; }
    public String getLogFolder() { return logFolder; }
    public String getFallbackLogName() { return fallbackLogName; }
    public String getDirectory() { return directory; }
    public String getActiveMqUrl() { return activeMqUrl; }
    public String getQueue() { return queue; }
    public String getCredentials() { return credentials; }
    public String getLogLevel() { return logLevel; }
    public boolean isCheckHashBeforeCopy() { return checkHashBeforeCopy; }
=======
>>>>>>> ca2b6794429a0d7a23ab3c1fd123d6a23daa40f5
}