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

    public static AppConfig load(InputStream input) {
        Properties props = loadProperties(input);

        List<String> supportedExtensions = parseStringToList(
                props.getProperty("supportedExtensions", "csv,txt")
        );

        String logFolder = requireNonBlank(props.getProperty("logFolder"), "logFolder");
        String fallbackLogName = requireNonBlank(props.getProperty("fallbackLogName"), "fallbackLogName");

        Credentials credentials = parseCredentials(
                requireNonBlank(props.getProperty("credentials", "INPUTT/123456"), "credentials")
        );

        Level logLevel = parseLogLevel(props.getProperty("logLevel", "INFO"));

        Path directory = resolvePath(requireNonBlank(props.getProperty("directory", "~/S_FILE_UPLOADER"), "directory"));

        URI activeMqUri = parseURI(requireNonBlank(props.getProperty("activeMqUrl"), "activeMqUrl"));

        String queue = requireNonBlank(props.getProperty("queue", "java:/queue/t24DSPPACKAGERQueue"), "queue");

        boolean checkHashBeforeCopy = Boolean.parseBoolean(
                props.getProperty("checkHashBeforeCopy", "true")
        );

        try {
            Files.createDirectories(directory);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create/access directory: " + directory, e);
        }

        return new AppConfig(
                supportedExtensions,
                logFolder,
                fallbackLogName,
                credentials,
                logLevel,
                directory,
                activeMqUri,
                queue,
                checkHashBeforeCopy
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