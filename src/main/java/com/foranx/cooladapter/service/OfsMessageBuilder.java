package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.FolderConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class OfsMessageBuilder {

    private final FolderConfig folderConfig;
    private final AppConfig appConfig;

    public OfsMessageBuilder(FolderConfig folderConfig, AppConfig appConfig) {
        this.folderConfig = folderConfig;
        this.appConfig = appConfig;
    }

    public List<String> build(Map<String, List<Object>> data) {
        if (data.isEmpty()) return List.of();

        List<String> headers = new ArrayList<>(data.keySet());
        if (headers.isEmpty()) return List.of();

        String idHeader = headers.get(0);
        int recordCount = data.get(idHeader).size();
        List<String> messages = new ArrayList<>(recordCount);

        for (int i = 0; i < recordCount; i++) {
            messages.add(createSingleMessage(i, headers, data));
        }
        return messages;
    }

    private String createSingleMessage(int rowIndex, List<String> headers, Map<String, List<Object>> data) {
        // Формируем заголовок сообщения: VERSION/I/PROCESS,USER/PASS,ID
        String prefix = String.join(",",
                folderConfig.tableVersion(),
                folderConfig.tableVersion() + "/I/PROCESS",
                appConfig.credentials().username() + "/******", // Пароль лучше не светить, либо брать из конфига
                data.get(headers.get(0)).get(rowIndex).toString()
        );
        // В оригинале было credentials().username(), но T24 обычно требует и пароль в OFS.
        // Если у вас авторизация внешняя, оставьте только username.

        StringJoiner body = new StringJoiner(",");

        // Пропускаем ID (индекс 0), идем по полям данных
        for (int h = 1; h < headers.size(); h++) {
            String headerName = headers.get(h);
            Object valueObj = data.get(headerName).get(rowIndex);
            appendFieldData(body, headerName, valueObj);
        }

        return prefix + "," + body.toString();
    }

    private void appendFieldData(StringJoiner joiner, String headerName, Object value) {
        if (value instanceof List<?> list) {
            processListValue(joiner, headerName, list);
        } else {
            // Простые поля: FIELD=VALUE
            joiner.add(headerName + "=" + value);
        }
    }

    private void processListValue(StringJoiner joiner, String headerName, List<?> list) {
        if (list.isEmpty()) return;

        // Проверяем, является ли это MultiValue (List<List<String>>) или SubValue (List<String>)
        if (list.get(0) instanceof List) {
            // Это MultiValue поле (набор SubValue)
            @SuppressWarnings("unchecked")
            List<List<String>> multiValueList = (List<List<String>>) list;

            for (int m = 0; m < multiValueList.size(); m++) {
                List<String> subValues = multiValueList.get(m);
                for (int s = 0; s < subValues.size(); s++) {
                    // FIELD:M:S=VALUE
                    joiner.add(String.format("%s:%d:%d=%s", headerName, m + 1, s + 1, subValues.get(s)));
                }
            }
        } else {
            // Это просто SubValue поле (без мультизначений)
            @SuppressWarnings("unchecked")
            List<String> subValueList = (List<String>) list;

            for (int s = 0; s < subValueList.size(); s++) {
                // FIELD:S=VALUE (или FIELD:1:S=VALUE в зависимости от версии T24, оставил как в оригинале)
                joiner.add(String.format("%s:%d=%s", headerName, s + 1, subValueList.get(s)));
            }
        }
    }
}