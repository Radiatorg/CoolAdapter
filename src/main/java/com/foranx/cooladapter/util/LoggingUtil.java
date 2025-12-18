package com.foranx.cooladapter.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.*;

public class LoggingUtil {

    private LoggingUtil() {}

    public static void configure(String logFilePath) {
        try {
            if (logFilePath == null || logFilePath.isBlank()) {
                throw new IllegalArgumentException("logFilePath is null or blank");
            }

            Path logPath = Path.of(logFilePath);
            Path parent = logPath.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }

            LogManager.getLogManager().reset();
            Logger root = Logger.getLogger("");
            root.setLevel(Level.INFO);

            FileHandler fh = new FileHandler(logPath.toString(), true);
            fh.setFormatter(new SimpleFormatter());
            root.addHandler(fh);

        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize logging", e);
        }
    }
}