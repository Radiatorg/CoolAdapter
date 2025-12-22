package com.foranx.cooladapter.handler;

import java.util.List;
import java.util.stream.Collectors;

public class LowerCaseHandler implements ValueHandler {
    @Override
    public Object handle(Object value) {
        if (value instanceof String s) return s.toLowerCase();
        if (value instanceof List<?> list) {
            return list.stream().map(v -> v.toString().toLowerCase()).collect(Collectors.toList());
        }
        return value;
    }
}