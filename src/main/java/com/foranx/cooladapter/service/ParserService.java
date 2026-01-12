package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.FolderConfig;
import com.foranx.cooladapter.handler.HandlerFactory;
import com.foranx.cooladapter.handler.ValueHandler;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class ParserService {

    public static void validateStructure(Path file, FolderConfig config) throws IOException {
        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {

            String fieldDelimPattern = Pattern.quote(config.fieldDelimiter());
            int expectedColumns = config.headers().size();

            String line;
            int lineNum = 0;

            // Если первая строка - хедер, читаем её для проверки или пропускаем
            if (config.isFirstLineHeader()) {
                String headerLine = reader.readLine();
                lineNum++;
                if (headerLine == null) return; // Пустой файл - ок или ошибка?

                // Если заголовки берутся из файла, нужно посчитать их количество тут
                if (expectedColumns == 0) { // Если в конфиге заголовков нет
                    expectedColumns = headerLine.split(fieldDelimPattern, -1).length;
                }
            }

            while ((line = reader.readLine()) != null) {
                lineNum++;
                if (line.trim().isEmpty()) continue;

                // -1 в split важен, чтобы считать пустые хвосты (,,,)
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

    /**
     * Читает файл построчно и вызывает rowProcessor для каждой распарсенной строки.
     * Память не забивается, так как данные не копятся.
     */
    public static void parseStream(Path file, FolderConfig config, Consumer<Map<String, Object>> rowProcessor) throws IOException {

        try (BufferedReader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {

            List<String> headers;

            // 1. Читаем заголовок
            if (config.isFirstLineHeader()) {
                String headerLine = reader.readLine();
                if (headerLine == null) return; // Пустой файл

                String[] headerParts = headerLine.split(Pattern.quote(config.fieldDelimiter()));
                headers = new ArrayList<>();
                for (String h : headerParts) {
                    headers.add(h.trim());
                }
            } else {
                headers = config.headers();
            }

            // Предкомпиляция разделителей для скорости
            String multiDelim = config.multiValueDelimiter();
            String subDelim = config.subValueDelimiter();
            boolean hasMultiDelim = multiDelim != null && !multiDelim.isEmpty();
            boolean hasSubDelim = subDelim != null && !subDelim.isEmpty();

            String fieldDelimPattern = Pattern.quote(config.fieldDelimiter());
            String multiDelimPattern = hasMultiDelim ? Pattern.quote(multiDelim) : null;
            String subDelimPattern = hasSubDelim ? Pattern.quote(subDelim) : null;

            // Кэшируем хендлеры, чтобы не искать их на каждой строке
            Map<String, ValueHandler> activeHandlers = resolveHandlers(headers, config);

            String line;
            // 2. Бежим по строкам
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String[] fields = line.split(fieldDelimPattern, -1);

                // LinkedHashMap сохраняет порядок полей, что важно для OFS
                Map<String, Object> rowData = new LinkedHashMap<>();

                for (int colIndex = 0; colIndex < headers.size(); colIndex++) {
                    String headerName = headers.get(colIndex);
                    String rawValue = (colIndex < fields.length) ? fields[colIndex].trim() : "";

                    Object processedValue;

                    // Логика Multi/Sub values
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

                    // Применяем Handler тут же, к конкретной ячейке
                    if (activeHandlers.containsKey(headerName)) {
                        processedValue = activeHandlers.get(headerName).handle(processedValue);
                    }

                    rowData.put(headerName, processedValue);
                }

                // 3. Отдаем готовую строку на обработку и ЗАБЫВАЕМ её
                rowProcessor.accept(rowData);
            }
        }
    }

    private static Map<String, ValueHandler> resolveHandlers(List<String> headers, FolderConfig config) {
        Map<String, ValueHandler> result = new HashMap<>();
        Map<String, String> handlerConfigs = config.handlers();

        for (String header : headers) {
            String handlerClassName = handlerConfigs.get("handler." + header);
            if (handlerClassName != null) {
                ValueHandler handler = HandlerFactory.getHandler(handlerClassName);
                if (handler != null) {
                    result.put(header, handler);
                }
            }
        }
        return result;
    }
}