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

    public String buildSingleMessage(Map<String, Object> rowData) {
        if (rowData == null || rowData.isEmpty()) return null;

        List<String> headers = rowData.keySet().stream().toList();
        if (headers.isEmpty()) return null;

        String idHeader = headers.getFirst();
        Object idValueObj = rowData.get(idHeader);
        String idValue = (idValueObj != null) ? idValueObj.toString() : "";

        String operation = folderConfig.tableVersion();
        String options = "/I/PROCESS";

        String password = new String(appConfig.credentials().password());
        String userInfo = appConfig.credentials().username() + "/" + password;

        String prefix = String.join(",",
                operation,
                options,
                userInfo,
                idValue
        );

        StringJoiner body = new StringJoiner(",");

        for (int h = 1; h < headers.size(); h++) {
            String headerName = headers.get(h);
            Object valueObj = rowData.get(headerName);
            appendFieldData(body, headerName, valueObj);
        }

        return prefix + "," + body;
    }

    private void appendFieldData(StringJoiner joiner, String headerName, Object value) {
        if (value == null) return;

        if (value instanceof List<?> list) {
            processListValue(joiner, headerName, list, 1);
        } else {
            String strVal = value.toString();
            if (!strVal.isEmpty()) {
                joiner.add(headerName + "=" + escapeOfsData(strVal));
            }
        }
    }

    private void processListValue(StringJoiner joiner, String headerName, List<?> list, int multiIndex) {
        if (list.isEmpty()) return;

        Object first = list.getFirst();

        if (first instanceof List) {
            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                if (item instanceof List<?> subList) {
                    processSubValues(joiner, headerName, subList, i + 1);
                }
            }
        } else {

            for (int i = 0; i < list.size(); i++) {
                Object item = list.get(i);
                if (item == null) continue;

                if (item instanceof List) {
                    processSubValues(joiner, headerName, (List<?>) item, i + 1);
                } else {
                    String val = item.toString();
                    joiner.add(String.format("%s:%d=%s", headerName, i + 1, escapeOfsData(val)));
                }
            }
        }
    }

    private void processSubValues(StringJoiner joiner, String headerName, List<?> subList, int multiIndex) {
        for (int s = 0; s < subList.size(); s++) {
            Object item = subList.get(s);
            if (item != null) {
                String val = item.toString();
                joiner.add(String.format("%s:%d:%d=%s", headerName, multiIndex, s + 1, escapeOfsData(val)));
            }
        }
    }

    private String escapeOfsData(String input) {
        if (input == null) return "";
        return input.replace(",", " ");
    }
}