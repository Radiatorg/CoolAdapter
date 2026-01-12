package com.foranx.cooladapter.config;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;

public final class AppConfigLoader {

    private AppConfigLoader() {}

    private static final String PROP_SUPPORTED_EXTENSIONS = "supportedExtensions";
    private static final String PROP_LOG_FOLDER = "logFolder";
    private static final String PROP_FALLBACK_LOG_NAME = "fallbackLogName";
    private static final String PROP_CREDENTIALS = "credentials";
    private static final String PROP_LOG_LEVEL = "logLevel";
    private static final String PROP_DIRECTORY = "directory";
    private static final String PROP_ACTIVEMQ_URL = "activeMqUrl";
    private static final String PROP_QUEUE = "queue";
    private static final String PROP_CHECK_HASH = "checkHashBeforeCopy";
    private static final String PROP_FILE_STABILITY = "fileStabilityThreshold";
    private static final String PROP_MAX_FILE_WAIT_TIME = "maxFileWaitTime";

    private static final String DEFAULT_SUPPORTED_EXTENSIONS = "csv,txt";
    private static final String DEFAULT_CREDENTIALS = "INPUTT/123456";
    private static final String DEFAULT_LOG_LEVEL = "INFO";
    private static final String DEFAULT_ACTIVE_MQ_URL = "tcp://192.168.38.3:5445";
    private static final String DEFAULT_DIRECTORY = "~/S_FILE_UPLOADER";
    private static final String DEFAULT_QUEUE = "java:/queue/t24DSPPACKAGERQueue";
    private static final String DEFAULT_CHECK_HASH = "true";
    private static final String DEFAULT_FILE_STABILITY = "2000";
    private static final String DEFAULT_MAX_FILE_WAIT_TIME = "900000";

    public static AppConfig load(InputStream input) {
        Properties props = loadProperties(input);

        List<String> supportedExtensions = parseStringToList(
                props.getProperty(PROP_SUPPORTED_EXTENSIONS, DEFAULT_SUPPORTED_EXTENSIONS)
        );

        String logFolder = requireNonBlank(
                props.getProperty(PROP_LOG_FOLDER),
                PROP_LOG_FOLDER
        );

        String fallbackLogName = requireNonBlank(
                props.getProperty(PROP_FALLBACK_LOG_NAME),
                PROP_FALLBACK_LOG_NAME
        );

        Credentials credentials = parseCredentials(
                requireNonBlank(
                        props.getProperty(PROP_CREDENTIALS, DEFAULT_CREDENTIALS),
                        PROP_CREDENTIALS
                )
        );

        Level logLevel = parseLogLevel(
                props.getProperty(PROP_LOG_LEVEL, DEFAULT_LOG_LEVEL)
        );

        Path directory = resolvePath(
                requireNonBlank(
                        props.getProperty(PROP_DIRECTORY, DEFAULT_DIRECTORY),
                        PROP_DIRECTORY
                )
        );

        URI activeMqUri = parseURI(
                requireNonBlank(
                        props.getProperty(PROP_ACTIVEMQ_URL, DEFAULT_ACTIVE_MQ_URL),
                        PROP_ACTIVEMQ_URL
                )
        );

        String queue = requireNonBlank(
                props.getProperty(PROP_QUEUE, DEFAULT_QUEUE),
                PROP_QUEUE
        );

        boolean checkHashBeforeCopy = Boolean.parseBoolean(
                props.getProperty(PROP_CHECK_HASH, DEFAULT_CHECK_HASH)
        );

        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create/access directory: " + directory, e);
        }

        long stabilityThreshold = Long.parseLong(props.getProperty(PROP_FILE_STABILITY, DEFAULT_FILE_STABILITY));
        long maxWaitTime = Long.parseLong(props.getProperty(PROP_MAX_FILE_WAIT_TIME, DEFAULT_MAX_FILE_WAIT_TIME));

        return new AppConfig(
                supportedExtensions,
                logFolder,
                fallbackLogName,
                credentials,
                logLevel,
                directory,
                activeMqUri,
                queue,
                checkHashBeforeCopy,
                stabilityThreshold,
                maxWaitTime
        );
    }

    private static Properties loadProperties(InputStream input) {
        if (input == null) {
            throw new IllegalStateException("application.properties not found in classpath");
        }

        Properties props = new Properties();
        try {
            props.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application.properties", e);
        }
        return props;
    }

    private static List<String> parseStringToList(String value) {
        if (value == null || value.isBlank()) return List.of();

        return Arrays.stream(value.split(","))
                .map(String::trim)
                .map(String::toLowerCase)
                .filter(s -> !s.isEmpty())
                .distinct()
                .toList();
    }

    private static Path resolvePath(String path) {
        String resolved = path.startsWith("~")
                ? path.replaceFirst("~", System.getProperty("user.home"))
                : path;
        return Paths.get(resolved).toAbsolutePath().normalize();
    }

    private static Credentials parseCredentials(String raw) {
        if (!raw.contains("/")) {
            throw new IllegalStateException("'credentials' must be in format 'user/password'");
        }

        String[] parts = raw.split("/", 2);
        return new Credentials(parts[0].trim(), parts[1].trim().toCharArray());
    }

    private static Level parseLogLevel(String levelStr) {
        try {
            return Level.parse(levelStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid logLevel value: " + levelStr, e);
        }
    }

    private static URI parseURI(String uriStr) {
        try {
            return URI.create(uriStr);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Invalid URI: " + uriStr, e);
        }
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("'" + name + "' must be set and non-empty");
        }
        return value;
    }
}