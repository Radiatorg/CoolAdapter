package com.foranx.cooladapter.config;

import java.util.*;
import java.util.stream.Collectors;

public class FolderConfig {

    private boolean isFirstLineHeader = false;
    private String subValueDelimiter;
    private String multiValueDelimiter;
    private String fieldDelimiter = ",";
    private String recordDelimiter = "\n";
    private List<String> headers = new ArrayList<>();
    private String logFileName;
    private final Map<String, String> handlers = new LinkedHashMap<>();
    private String tableVersion;

    private FolderConfig() {}

    public static FolderConfig fromProperties(Properties props, String folderName) {
        FolderConfig config = new FolderConfig();

        String firstLineHeaderStr = props.getProperty("isFirstLineHeader");
        if (firstLineHeaderStr != null) {
            config.isFirstLineHeader = Boolean.parseBoolean(firstLineHeaderStr);
        }

        config.subValueDelimiter = props.getProperty("subValueDelimiter");
        config.multiValueDelimiter = props.getProperty("multiValueDelimiter");
        config.fieldDelimiter = props.getProperty("fieldDelimiter", ",");
        config.recordDelimiter = props.getProperty("recordDelimiter", "\n")
                .replace("\\n", "\n")
                .replace("\\r", "\r");

        String headersStr = props.getProperty("headers");
        if (headersStr != null && !headersStr.isBlank()) {
            config.headers = Arrays.stream(headersStr.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }

        config.logFileName = props.getProperty("logFileName");
        if (config.logFileName == null || config.logFileName.isBlank()) {
            config.logFileName = folderName + ".log";
        }

        config.tableVersion = props.getProperty("tableVersion");

        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("handler.")) {
                config.handlers.put(key, props.getProperty(key));
            }
        }
        if (config.handlers.isEmpty()) {
            config.handlers.put("handler.default", "com.temenos.handlers.Test.java");
        }

        config.validate();
        return config;
    }

    private void validate() {
        if (!isFirstLineHeader && (headers == null || headers.isEmpty())) {
            throw new IllegalStateException("Headers are mandatory when 'isFirstLineHeader' is false.");
        }

        if (tableVersion == null || tableVersion.isBlank()) {
            throw new IllegalStateException("Property 'tableVersion' is mandatory in folder config. It defines the T24 application.");
        }
    }

    public boolean isFirstLineHeader() { return isFirstLineHeader; }
    public String getSubValueDelimiter() { return subValueDelimiter; }
    public String getMultiValueDelimiter() { return multiValueDelimiter; }
    public String getFieldDelimiter() { return fieldDelimiter; }
    public String getRecordDelimiter() { return recordDelimiter; }
    public List<String> getHeaders() { return headers; }
    public String getLogFileName() { return logFileName; }
    public Map<String, String> getHandlers() { return handlers; }
    public String getTableVersion() { return tableVersion; }
}