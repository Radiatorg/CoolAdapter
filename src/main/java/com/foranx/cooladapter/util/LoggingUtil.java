package com.foranx.cooladapter.util;

import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.*;

public class LoggingUtil {

    private static final List<Handler> managedHandlers = new CopyOnWriteArrayList<>();
    private static final int LOG_FILE_LIMIT = 10 * 1024 * 1024;
    private static final int LOG_FILE_COUNT = 5;

    private static Logger configuredLogger;

    private LoggingUtil() {}

    public static synchronized void configure(String logFilePath, String logLevelStr) {
        closeHandlers();

        try {
            if (logFilePath == null || logFilePath.isBlank()) {
                throw new IllegalArgumentException("logFilePath is null or blank");
            }

            Path logPath = Path.of(logFilePath);
            if (logPath.getParent() != null) {
                Files.createDirectories(logPath.getParent());
            }

            Level level = parseLevel(logLevelStr);

            configuredLogger = Logger.getLogger("com.foranx.cooladapter");
            configuredLogger.setLevel(level);
            configuredLogger.setUseParentHandlers(false);

            Formatter formatter = new SingleLineFormatter();

            String pattern = logFilePath + ".%g";
            FileHandler fileHandler = new FileHandler(pattern, LOG_FILE_LIMIT, LOG_FILE_COUNT, true);
            fileHandler.setEncoding(StandardCharsets.UTF_8.name());
            fileHandler.setFormatter(formatter);
            fileHandler.setLevel(level);

            configuredLogger.addHandler(fileHandler);
            managedHandlers.add(fileHandler);

            Handler stdoutHandler = getHandler(formatter, level);

            configuredLogger.addHandler(stdoutHandler);
            managedHandlers.add(stdoutHandler);

        } catch (IOException | SecurityException e) {
            Logger.getAnonymousLogger().log(Level.SEVERE, "!!! FAILED TO INITIALIZE LOGGING !!!", e);
        }
    }

    private static @NotNull Handler getHandler(Formatter formatter, Level level) throws UnsupportedEncodingException {
        Handler stdoutHandler = new StreamHandler(System.out, formatter) {
            @Override
            public synchronized void publish(LogRecord record) {
                super.publish(record);
                flush();
            }

            @Override
            public synchronized void close() throws SecurityException {
                flush();
            }
        };
        stdoutHandler.setEncoding(StandardCharsets.UTF_8.name());
        stdoutHandler.setLevel(level);
        return stdoutHandler;
    }

    public static synchronized void closeHandlers() {
        if (configuredLogger != null) {
            for (Handler handler : managedHandlers) {
                try {
                    handler.close();
                } catch (Exception ignored) {}
                configuredLogger.removeHandler(handler);
            }
        }
        managedHandlers.clear();
    }

    private static Level parseLevel(String logLevelStr) {
        if (logLevelStr == null || logLevelStr.isBlank()) return Level.INFO;
        try {
            return Level.parse(logLevelStr.toUpperCase());
        } catch (IllegalArgumentException e) {
            return Level.INFO;
        }
    }

    private static class SingleLineFormatter extends Formatter {
        private static final DateTimeFormatter DATE_FORMAT =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        @Override
        public String format(LogRecord record) {
            StringBuilder sb = new StringBuilder(128);

            ZonedDateTime dt = ZonedDateTime.ofInstant(record.getInstant(), ZoneId.systemDefault());
            sb.append(DATE_FORMAT.format(dt)).append(" ");

            String level = record.getLevel().getName();
            String threadName = Thread.currentThread().getName();

            sb.append(String.format("[%-7s] [%-15.15s] ", level, threadName));

            sb.append(formatMessage(record));
            sb.append(System.lineSeparator());

            if (record.getThrown() != null) {
                try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
                    record.getThrown().printStackTrace(pw);
                    sb.append(sw);
                } catch (IOException ex) {
                    // ignore
                }
            }
            return sb.toString();
        }
    }
}