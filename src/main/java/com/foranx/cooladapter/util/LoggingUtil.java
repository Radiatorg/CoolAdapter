package com.foranx.cooladapter.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.*;

public class LoggingUtil {

    private static final List<Handler> managedHandlers = new ArrayList<>();

    private LoggingUtil() {}

    public static void configure(String logFilePath, String logLevelStr) {
        closeHandlers();

        try {
            if (logFilePath == null || logFilePath.isBlank()) {
                throw new IllegalArgumentException("logFilePath is null or blank");
            }

            Path logPath = Path.of(logFilePath);
            Path parent = logPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            Level level = Level.INFO;
            if (logLevelStr != null && !logLevelStr.isBlank()) {
                try {
                    level = Level.parse(logLevelStr.toUpperCase());
                } catch (IllegalArgumentException e) {
                    System.err.println("Invalid logLevel in properties: " + logLevelStr + ". Using default INFO.");
                }
            }

            Logger appLogger = Logger.getLogger("com.foranx.cooladapter");
            appLogger.setLevel(level);
            appLogger.setUseParentHandlers(false);

            FileHandler fileHandler = new FileHandler(logPath.toString(), true);
            fileHandler.setFormatter(new SimpleFormatter());
            fileHandler.setLevel(level);
            appLogger.addHandler(fileHandler);
            managedHandlers.add(fileHandler);

            ConsoleHandler consoleHandler = new ConsoleHandler();
            consoleHandler.setLevel(level);
            appLogger.addHandler(consoleHandler);
            managedHandlers.add(consoleHandler);

        } catch (IOException | SecurityException e) {
            System.err.println("!!! FAILED TO INITIALIZE LOGGING !!!");
            e.printStackTrace();
            throw new RuntimeException("Failed to initialize logging", e);
        }
    }

    public static void closeHandlers() {
        Logger appLogger = Logger.getLogger("com.foranx.cooladapter");
        for (Handler handler : managedHandlers) {
            handler.close();
            appLogger.removeHandler(handler);
        }
        managedHandlers.clear();
    }
}