package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.FolderConfig;
import com.foranx.cooladapter.handler.HandlerFactory;
import com.foranx.cooladapter.handler.ValueHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class ParserService {

    public static void validateStructure(Path file, FolderConfig config) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, config.charset())) {

            String fieldDelimPattern = Pattern.quote(config.fieldDelimiter());
            int expectedColumns = config.headers().size();

            String line;
            int lineNum = 0;

            if (config.isFirstLineHeader()) {
                String headerLine = reader.readLine();
                lineNum++;
                if (headerLine == null) return;

                if (expectedColumns == 0) {
                    expectedColumns = headerLine.split(fieldDelimPattern, -1).length;
                }
            }

            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty()) continue;

                String[] fields = line.split(fieldDelimPattern, -1);

                if (fields.length != expectedColumns) {
                    throw new IllegalStateException(String.format(
                            "Validation FAILED at line %d. Expected %d columns, found %d. Content: %s",
                            lineNum, expectedColumns, fields.length, line
                    ));
                }
            }
        }
    }

    public static void parseStream(Path file, FolderConfig config, Consumer<Map<String, Object>> rowProcessor) throws IOException {

        try (BufferedReader reader = Files.newBufferedReader(file, config.charset())) {

            List<String> headers;

            if (config.isFirstLineHeader()) {
                String headerLine = reader.readLine();
                if (headerLine == null) return;

                String[] headerParts = headerLine.split(Pattern.quote(config.fieldDelimiter()));
                headers = new ArrayList<>();
                for (String h : headerParts) {
                    headers.add(h.trim());
                }
            } else {
                headers = config.headers();
            }

            String multiDelim = config.multiValueDelimiter();
            String subDelim = config.subValueDelimiter();
            boolean hasMultiDelim = multiDelim != null && !multiDelim.isEmpty();
            boolean hasSubDelim = subDelim != null && !subDelim.isEmpty();

            String fieldDelimPattern = Pattern.quote(config.fieldDelimiter());
            String multiDelimPattern = hasMultiDelim ? Pattern.quote(multiDelim) : null;
            String subDelimPattern = hasSubDelim ? Pattern.quote(subDelim) : null;

            Map<String, ValueHandler> activeHandlers = resolveHandlers(headers, config);

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String[] fields = line.split(fieldDelimPattern, -1);

                Map<String, Object> rowData = new LinkedHashMap<>();

                for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
                    String headerName = headers.get(colIndex);
                    String rawValue = (colIndex < fields.length) ? fields[colIndex].trim() : "";

                    Object processedValue;

                    if (hasMultiDelim && rawValue.contains(multiDelim)) {
                        List<List<String>> multiValueList = new ArrayList<>();
                        String[] multiParts = rawValue.split(multiDelimPattern);
                        for (String part : multiParts) {
                            if (hasSubDelim && part.contains(subDelim)) {
                                multiValueList.add(Arrays.asList(part.split(subDelimPattern)));
                            } else {
                                multiValueList.add(List.of(part));
                            }
                        }
                        processedValue = multiValueList;
                    } else if (hasSubDelim && rawValue.contains(subDelim)) {
                        processedValue = Arrays.asList(rawValue.split(subDelimPattern));
                    } else {
                        processedValue = rawValue;
                    }

                    if (activeHandlers.containsKey(headerName)) {
                        processedValue = activeHandlers.get(headerName).handle(processedValue);
                    }

                    rowData.put(headerName, processedValue);
                }

                rowProcessor.accept(rowData);
            }
        }
    }

    private static Map<String, ValueHandler> resolveHandlers(List<String> headers, FolderConfig config) {
        Map<String, ValueHandler> result = new HashMap<>();
        Map<String, String> handlerConfigs = config.handlers();
        Map<String, String> handlerPaths = config.handlerPaths();
        String globalPath = config.defaultHandlerPath();

        for (String header : headers) {
            String handlerKey = "handler." + header;
            String handlerClassName = handlerConfigs.get(handlerKey);

            if (handlerClassName != null) {
                String jarPath = handlerPaths.get(handlerKey);

                if (jarPath == null || jarPath.isBlank()) {
                    jarPath = globalPath;
                }

                ValueHandler handler = HandlerFactory.getHandler(handlerClassName, jarPath);

                if (handler != null) {
                    result.put(header, handler);
                }
            }
        }
        return result;
    }
}