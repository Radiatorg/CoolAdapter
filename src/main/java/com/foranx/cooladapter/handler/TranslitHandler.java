package com.foranx.cooladapter.handler;

import java.util.HashMap;
import java.util.Map;

public class TranslitHandler implements ValueHandler {
    private static final Map<Character, String> charMap = new HashMap<>();

    static {
        charMap.put('а', "a"); charMap.put('б', "b"); charMap.put('в', "v");
        charMap.put('г', "g"); charMap.put('д', "d"); charMap.put('е', "e");
        charMap.put('ё', "e"); charMap.put('ж', "zh"); charMap.put('з', "z");
        charMap.put('и', "i"); charMap.put('й', "y"); charMap.put('к', "k");
        charMap.put('л', "l"); charMap.put('м', "m"); charMap.put('н', "n");
        charMap.put('о', "o"); charMap.put('п', "p"); charMap.put('р', "r");
        charMap.put('с', "s"); charMap.put('т', "t"); charMap.put('у', "u");
        charMap.put('ф', "f"); charMap.put('х', "h"); charMap.put('ц', "ts");
        charMap.put('ч', "ch"); charMap.put('ш', "sh"); charMap.put('щ', "shch");
        charMap.put('ы', "y"); charMap.put('ь', ""); charMap.put('э', "e");
        charMap.put('ю', "yu"); charMap.put('я', "ya");
    }

    @Override
    public Object handle(Object value) {
        if (!(value instanceof String s)) return value;

        StringBuilder sb = new StringBuilder();
        for (char c : s.toLowerCase().toCharArray()) {
            sb.append(charMap.getOrDefault(c, String.valueOf(c)));
        }
        return sb.toString().toUpperCase(); // T24 обычно требует UPPERCASE
    }
}