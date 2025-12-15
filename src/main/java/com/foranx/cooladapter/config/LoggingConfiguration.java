package com.foranx.cooladapter.config;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.*;

public class LoggingConfiguration {

    private LoggingConfiguration() {}

    public static void init(String logFilePath, String logLevelStr) {
        try {
            if (logFilePath == null || logFilePath.isBlank()) {
                throw new IllegalArgumentException("logFilePath is null or blank");
            }

            Path logPath = Path.of(logFilePath);
            Path parent = logPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            Level userLevel = parseLevel(logLevelStr);

            Logger appLogger = Logger.getLogger("com.foranx");

            for (Handler h : appLogger.getHandlers()) {
                appLogger.removeHandler(h);
            }

            appLogger.setLevel(userLevel);
            appLogger.setUseParentHandlers(false);

            FileOutputStream fos = new FileOutputStream(logPath.toFile(), true);

            StreamHandler fileHandler = new StreamHandler(fos, new SimpleFormatter()) {
                @Override
                public synchronized void publish(LogRecord record) {
                    super.publish(record);
                    flush();
                }
            };

            fileHandler.setLevel(Level.ALL);

            appLogger.addHandler(fileHandler);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize logging", e);
        }
    }

    private static Level parseLevel(String levelStr) {
        if (levelStr == null || levelStr.isBlank()) {
            return Level.INFO;
        }

        String cleanLevel = levelStr.trim().toUpperCase();

        return switch (cleanLevel) {
            case "DEBUG" -> Level.FINE;
            case "TRACE" -> Level.FINEST;
            case "ERROR" -> Level.SEVERE;
            case "WARN"  -> Level.WARNING;
            case "INFO"  -> Level.INFO;
            default -> {
                try {
                    yield Level.parse(cleanLevel);
                } catch (IllegalArgumentException e) {
                    yield Level.INFO;
                }
            }
        };
    }
}