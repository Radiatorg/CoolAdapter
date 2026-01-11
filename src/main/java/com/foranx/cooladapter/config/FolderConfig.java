package com.foranx.cooladapter.config;

import java.util.*;
import java.util.logging.Logger;

public record FolderConfig(
        boolean isFirstLineHeader,
        String subValueDelimiter,
        String multiValueDelimiter,
        String fieldDelimiter,
        String recordDelimiter,
        List<String> headers,
        String logFileName,
        String fallbackLogFileName,
        Map<String, String> handlers,
        String tableVersion,
        int gtsControl
) {
    private static final Logger log = Logger.getLogger(FolderConfig.class.getName());

    private static final String PROP_GTS_CONTROL = "gtsControl";
    private static final String DEFAULT_GTS_CONTROL = "1";
    private static final Set<Integer> VALID_GTS_VALUES = Set.of(1, 2, 3, 4);

    private static final String PROP_FIRST_LINE_HEADER = "isFirstLineHeader";
    private static final String PROP_SUB_VALUE_DELIMITER = "subValueDelimiter";
    private static final String PROP_MULTI_VALUE_DELIMITER = "multiValueDelimiter";
    private static final String PROP_FIELD_DELIMITER = "fieldDelimiter";
    private static final String PROP_RECORD_DELIMITER = "recordDelimiter";
    private static final String PROP_HEADERS = "headers";
    private static final String PROP_LOG_FILE_NAME = "logFileName";
    private static final String PROP_FALLBACK_LOG_FILE_NAME = "fallbackLogFileName";
    private static final String PROP_TABLE_VERSION = "tableVersion";
    private static final String HANDLER_PREFIX = "handler.";

    private static final String DEFAULT_FIELD_DELIMITER = ",";
    private static final String DEFAULT_RECORD_DELIMITER = "\n";
    private static final String DEFAULT_FALLBACK_LOG_FILE = "fallback.log";
    private static final String DEFAULT_HANDLER = "com.temenos.handlers.Test.java";

    public FolderConfig {
        if (!isFirstLineHeader && (headers == null || headers.isEmpty())) {
            throw new IllegalStateException(
                    "FolderConfig: 'headers' must be specified when isFirstLineHeader=false"
            );
        }

        List<String> delimiters = new ArrayList<>();
        if (subValueDelimiter != null) delimiters.add(subValueDelimiter);
        if (fieldDelimiter != null) delimiters.add(fieldDelimiter);
        if (recordDelimiter != null) delimiters.add(recordDelimiter);

        long uniqueCount = delimiters.stream().distinct().count();
        if (uniqueCount < delimiters.size()) {
            throw new IllegalStateException(
                    "FolderConfig: 'subValueDelimiter', 'fieldDelimiter' and 'recordDelimiter' must be different"
            );
        }
    }

    public static FolderConfig fromProperties(Properties props, String folderName) {

        boolean isFirstLineHeader = Boolean.parseBoolean(
                props.getProperty(PROP_FIRST_LINE_HEADER, "false")
        );

        String subValueDelimiter = props.getProperty(PROP_SUB_VALUE_DELIMITER);
        String multiValueDelimiter = props.getProperty(PROP_MULTI_VALUE_DELIMITER);

        String fieldDelimiter = props.getProperty(
                PROP_FIELD_DELIMITER, DEFAULT_FIELD_DELIMITER
        );

        String recordDelimiter = normalizeDelimiter(
                props.getProperty(PROP_RECORD_DELIMITER, DEFAULT_RECORD_DELIMITER)
        );

        List<String> headers = parseHeaders(props.getProperty(PROP_HEADERS));

        String logFileName = requireNonBlank(
                props.getProperty(PROP_LOG_FILE_NAME, folderName + ".log"),
                PROP_LOG_FILE_NAME
        );

        String fallbackLogFileName = requireNonBlank(
                props.getProperty(PROP_FALLBACK_LOG_FILE_NAME, DEFAULT_FALLBACK_LOG_FILE),
                PROP_FALLBACK_LOG_FILE_NAME
        );

        String tableVersion = requireNonBlank(
                props.getProperty(PROP_TABLE_VERSION),
                PROP_TABLE_VERSION
        );

        Map<String, String> handlers = parseHandlers(props);

        int gtsControl = parseGtsControl(
                props.getProperty(PROP_GTS_CONTROL, DEFAULT_GTS_CONTROL)
        );

        return new FolderConfig(
                isFirstLineHeader,
                subValueDelimiter,
                multiValueDelimiter,
                fieldDelimiter,
                recordDelimiter,
                headers,
                logFileName,
                fallbackLogFileName,
                handlers,
                tableVersion,
                gtsControl
        );
    }

    private static int parseGtsControl(String gtsControlValue) {
        int value;
        try {
            value = Integer.parseInt(gtsControlValue);
        } catch (NumberFormatException e) {
            throw new IllegalStateException(
                    "FolderConfig: '" + PROP_GTS_CONTROL + "' must be a valid integer. Found: '" + gtsControlValue + "'"
            );
        }

        if (!VALID_GTS_VALUES.contains(value)) {
            throw new IllegalStateException(
                    "FolderConfig: '" + PROP_GTS_CONTROL + "' must be one of " + VALID_GTS_VALUES + ". Found: '" + gtsControlValue + "'"
            );
        }
        return value;
    }

    private static List<String> parseHeaders(String headersValue) {
        if (headersValue == null || headersValue.isBlank()) {
            return List.of();
        }

        return Arrays.stream(headersValue.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private static Map<String, String> parseHandlers(Properties props) {
        Map<String, String> handlers = new LinkedHashMap<>();

        for (String key : props.stringPropertyNames()) {
            if (key.startsWith(HANDLER_PREFIX)) {
                handlers.put(key, props.getProperty(key));
            }
        }

        if (handlers.isEmpty()) {
            handlers.put(
                    HANDLER_PREFIX + "default",
                    DEFAULT_HANDLER
            );
        }

        return Collections.unmodifiableMap(handlers);
    }

    private static String normalizeDelimiter(String value) {
        return value
                .replace("\\r", "\r")
                .replace("\\n", "\n");
    }

    private static String requireNonBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(
                    "FolderConfig: '" + name + "' must be set and non-empty"
            );
        }
        return value;
    }
}