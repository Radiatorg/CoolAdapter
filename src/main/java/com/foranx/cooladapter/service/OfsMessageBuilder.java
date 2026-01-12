package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.FolderConfig;

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

    // Метод теперь принимает ОДНУ строку данных
    public String buildSingleMessage(Map<String, Object> rowData) {
        if (rowData.isEmpty()) return null;

        // Получаем ключи (headers) из самой map, так как LinkedHashMap сохраняет порядок
        List<String> headers = rowData.keySet().stream().toList();
        if (headers.isEmpty()) return null;

        String idHeader = headers.get(0);
        Object idValue = rowData.get(idHeader);

        // Формируем заголовок сообщения: VERSION/I/PROCESS,USER/PASS,ID
        String prefix = String.join(",",
                folderConfig.tableVersion(),
                folderConfig.tableVersion() + "/I/PROCESS",
                appConfig.credentials().username() + "/******",
                String.valueOf(idValue)
        );

        StringJoiner body = new StringJoiner(",");

        // Пропускаем ID (индекс 0), идем по полям данных
        for (int h = 1; h < headers.size(); h++) {
            String headerName = headers.get(h);
            Object valueObj = rowData.get(headerName);
            appendFieldData(body, headerName, valueObj);
        }

        return prefix + "," + body.toString();
    }

    private void appendFieldData(StringJoiner joiner, String headerName, Object value) {
        if (value instanceof List<?> list) {
            processListValue(joiner, headerName, list);
        } else {
            joiner.add(headerName + "=" + value);
        }
    }

    private void processListValue(StringJoiner joiner, String headerName, List<?> list) {
        if (list.isEmpty()) return;

        if (list.get(0) instanceof List) {
            // MultiValue
            @SuppressWarnings("unchecked")
            List<List<String>> multiValueList = (List<List<String>>) list;
            for (int m = 0; m < multiValueList.size(); m++) {
                List<String> subValues = multiValueList.get(m);
                for (int s = 0; s < subValues.size(); s++) {
                    joiner.add(String.format("%s:%d:%d=%s", headerName, m + 1, s + 1, subValues.get(s)));
                }
            }
        } else {
            // SubValue
            @SuppressWarnings("unchecked")
            List<String> subValueList = (List<String>) list;
            for (int s = 0; s < subValueList.size(); s++) {
                joiner.add(String.format("%s:%d=%s", headerName, s + 1, subValueList.get(s)));
            }
        }
    }
}