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
import java.util.stream.Collectors;

public class ParserService {

    private static String removeBom(String line) {
        if (line != null && !line.isEmpty() && line.charAt(0) == '\uFEFF') {
            return line.substring(1);
        }
        return line;
    }

    public static void validateStructure(Path file, FolderConfig config) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, config.charset())) {

            Pattern fieldPattern = Pattern.compile(Pattern.quote(config.fieldDelimiter()));
            int expectedColumns = config.headers().size();

            String line;
            int lineNum = 0;

            if (config.isFirstLineHeader()) {
                String headerLine = reader.readLine();
                headerLine = removeBom(headerLine);
                lineNum++;
                if (headerLine == null) return;
                if (expectedColumns == 0) {
                    expectedColumns = fieldPattern.split(headerLine, -1).length;
                }
            }

            boolean isFirstLineOfData = !config.isFirstLineHeader();

            while ((line = reader.readLine()) != null) {
                if (isFirstLineOfData) {
                    line = removeBom(line);
                    isFirstLineOfData = false;
                }
                lineNum++;
                if (line.isBlank()) continue;

                String[] fields = fieldPattern.split(line, -1);

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

            Pattern fieldPattern = Pattern.compile(Pattern.quote(config.fieldDelimiter()));
            List<String> headers;

            if (config.isFirstLineHeader()) {
                String headerLine = reader.readLine();
                headerLine = removeBom(headerLine);
                if (headerLine == null) return;
                String[] headerParts = fieldPattern.split(headerLine);
                headers = new ArrayList<>(headerParts.length);
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

            Pattern multiPattern = hasMultiDelim ? Pattern.compile(Pattern.quote(multiDelim)) : null;
            Pattern subPattern = hasSubDelim ? Pattern.compile(Pattern.quote(subDelim)) : null;

            String customRecordDelim = config.recordDelimiter().trim();
            boolean needsRecordTrim = !customRecordDelim.isEmpty() && !customRecordDelim.equals("\n") && !customRecordDelim.equals("\r\n");

            Map<String, ValueHandler> activeHandlers = resolveHandlers(headers, config);

            String line;
            boolean isFirstLineOfData = !config.isFirstLineHeader();

            while ((line = reader.readLine()) != null) {

                if (isFirstLineOfData) {
                    line = removeBom(line);
                    isFirstLineOfData = false;
                }

                if (line.isBlank()) continue;

                String[] fields = fieldPattern.split(line, -1);
                Map<String, Object> rowData = new LinkedHashMap<>(headers.size());

                for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
                    String headerName = headers.get(colIndex);
                    String rawValue = (colIndex < fields.length) ? fields[colIndex] : "";

                    if (colIndex == fields.length - 1 && needsRecordTrim) {
                        if (rawValue.endsWith(customRecordDelim)) {
                            rawValue = rawValue.substring(0, rawValue.length() - customRecordDelim.length());
                        }
                    }

                    rawValue = rawValue.trim();

                    Object processedValue;

                    if (hasMultiDelim && rawValue.contains(multiDelim)) {
                        String[] multiParts = multiPattern.split(rawValue);
                        List<Object> multiValueList = new ArrayList<>(multiParts.length);

                        for (String part : multiParts) {
                            if (hasSubDelim && part.contains(subDelim)) {
                                multiValueList.add(Arrays.asList(subPattern.split(part)));
                            } else {
                                multiValueList.add(part);
                            }
                        }
                        processedValue = multiValueList;

                    } else if (hasSubDelim && rawValue.contains(subDelim)) {
                        processedValue = Arrays.asList(subPattern.split(rawValue));
                    } else {
                        processedValue = rawValue;
                    }

                    ValueHandler handler = activeHandlers.get(headerName);
                    if (handler != null) {
                        processedValue = applyHandlerRecursively(handler, processedValue);
                    }

                    rowData.put(headerName, processedValue);
                }

                rowProcessor.accept(rowData);
            }
        }
    }

    private static Object applyHandlerRecursively(ValueHandler handler, Object value) {
        if (value == null) return null;

        if (value instanceof List<?> list) {
            return list.stream()
                    .map(item -> applyHandlerRecursively(handler, item))
                    .collect(Collectors.toList());
        }

        return handler.handle(value);
    }

    private static Map<String, ValueHandler> resolveHandlers(List<String> headers, FolderConfig config) {
        Map<String, ValueHandler> result = new HashMap<>(headers.size());
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