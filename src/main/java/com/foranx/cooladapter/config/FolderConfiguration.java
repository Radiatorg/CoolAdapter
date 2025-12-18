package com.foranx.cooladapter.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

public class FolderConfiguration {

    private boolean isFirstLineHeader = false;
    private String valueDelimiter;
    private String fieldDelimiter = ",";
    private String recordDelimiter = "\n";
    private List<String> headers = new ArrayList<>();
    private String logFileName;
    private final Map<String, String> handlers = new LinkedHashMap<>();
    private String gtsControl = "1";
    private String tableVersion;
    private static final String DEFAULT_HANDLER_CLASS = "com.temenos.handlers.Test.java";

    private FolderConfiguration() {}

    public static FolderConfiguration load(Path propertiesFile) {
        FolderConfiguration config = new FolderConfiguration();
        Properties props = new Properties();

        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties from " + propertiesFile, e);
        }

        config.parseProperties(props, propertiesFile.getParent().getFileName().toString());
        config.validate();
        return config;
    }

    private void parseProperties(Properties props, String folderName) {
        String firstLineHeaderStr = props.getProperty("isFirstLineHeader");
        if (firstLineHeaderStr != null) {
            this.isFirstLineHeader = Boolean.parseBoolean(firstLineHeaderStr);
        }

        this.valueDelimiter = props.getProperty("valueDelimiter");

        this.fieldDelimiter = props.getProperty("fieldDelimiter", ",");

        this.recordDelimiter = props.getProperty("recordDelimiter", "\n")
                .replace("\\n", "\n")
                .replace("\\r", "\r");

        String headersStr = props.getProperty("headers");
        if (headersStr != null && !headersStr.isBlank()) {
            this.headers = Arrays.stream(headersStr.split(","))
                    .map(String::trim)
                    .collect(Collectors.toList());
        }

        this.logFileName = props.getProperty("logFileName");
        if (this.logFileName == null || this.logFileName.isBlank()) {
            this.logFileName = folderName + ".log";
        }

        for (String key : props.stringPropertyNames()) {
            if (key.startsWith("handler.")) {
                this.handlers.put(key, props.getProperty(key));
            }
        }

        if (this.handlers.isEmpty()) {
            this.handlers.put("handler.default", DEFAULT_HANDLER_CLASS);
        }

        this.gtsControl = props.getProperty("gtsControl", "1");

        this.tableVersion = props.getProperty("tableVersion");
    }

    private void validate() {
        if (tableVersion == null || tableVersion.isBlank()) {
            throw new IllegalStateException("'tableVersion' is mandatory but missing or empty.");
        }

        if (!isFirstLineHeader && (headers == null || headers.isEmpty())) {
            throw new IllegalStateException("Headers are mandatory when 'isFirstLineHeader' is false.");
        }
    }

    @Override
    public String toString() {
        return "FolderConfiguration{" +
                "isFirstLineHeader=" + isFirstLineHeader +
                ", valueDelimiter='" + valueDelimiter + '\'' +
                ", fieldDelimiter='" + fieldDelimiter + '\'' +
                ", recordDelimiter='" + recordDelimiter.replace("\n", "\\n").replace("\r", "\\r") + '\'' +
                ", headers=" + headers +
                ", logFileName='" + logFileName + '\'' +
                ", handlers=" + handlers +
                ", gtsControl='" + gtsControl + '\'' +
                ", tableVersion='" + tableVersion + '\'' +
                '}';
    }

    public boolean isFirstLineHeader() { return isFirstLineHeader; }
    public String getValueDelimiter() { return valueDelimiter; }
    public String getFieldDelimiter() { return fieldDelimiter; }
    public String getRecordDelimiter() { return recordDelimiter; }
    public List<String> getHeaders() { return headers; }
    public String getLogFileName() { return logFileName; }
    public Map<String, String> getHandlers() { return handlers; }
    public String getGtsControl() { return gtsControl; }
    public String getTableVersion() { return tableVersion; }
}