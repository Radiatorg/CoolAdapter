package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.FolderConfig;
import com.foranx.cooladapter.handler.HandlerFactory;
import com.foranx.cooladapter.handler.ValueHandler;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Pattern;

public class ParserService {

    public static Map<String, List<Object>> parse(Path file, FolderConfig config) throws IOException {
        String content = Files.readString(file);

        if (content.isBlank()) {
            return Collections.emptyMap();
        }

        String[] records = content.split(Pattern.quote(config.getRecordDelimiter()));
        if (records.length == 0) {
            return Collections.emptyMap();
        }

        List<String> headers;
        int dataStartIndex = 0;

        if (config.isFirstLineHeader()) {
            String headerLine = records[0];
            String[] headerParts = headerLine.split(Pattern.quote(config.getFieldDelimiter()));
            headers = new ArrayList<>();
            for (String h : headerParts) {
                headers.add(h.trim());
            }
            dataStartIndex = 1;
        } else {
            headers = config.getHeaders();
        }

        Map<String, List<Object>> columns = new LinkedHashMap<>();
        for (String header : headers) {
            columns.put(header, new ArrayList<>());
        }

        String valDelim = config.getValueDelimiter();
        boolean hasValueDelimiter = valDelim != null && !valDelim.isEmpty();
        String valDelimRegex = hasValueDelimiter ? Pattern.quote(valDelim) : null;
        String fieldDelimRegex = Pattern.quote(config.getFieldDelimiter());

        for (int i = dataStartIndex; i < records.length; i++) {
            String record = records[i];
            if (record.trim().isEmpty()) continue;

            String[] fields = record.split(fieldDelimRegex, -1);

            for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
                if (colIndex >= headers.size()) break;

                String headerName = headers.get(colIndex);
                List<Object> columnData = columns.get(headerName);

                String rawValue = (colIndex < fields.length) ? fields[colIndex].trim() : "";

                if (hasValueDelimiter && rawValue.contains(valDelim)) {
                    String[] subValues = rawValue.split(valDelimRegex);
                    columnData.add(Arrays.asList(subValues));
                } else {
                    columnData.add(rawValue);
                }
            }
        }

        applyHandlers(columns, config);

        return columns;
    }


    private static void applyHandlers(Map<String, List<Object>> columns, FolderConfig config) {
        Map<String, String> handlerConfigs = config.getHandlers();

        columns.forEach((header, values) -> {
            String handlerClassName = handlerConfigs.get("handler." + header);

            if (handlerClassName != null) {
                ValueHandler handler = HandlerFactory.getHandler(handlerClassName);
                if (handler != null) {
                    values.replaceAll(handler::handle);
                }
            }
        });
    }
}