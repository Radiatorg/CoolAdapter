package com.foranx.cooladapter.handler;

/**
 * Пример: Проверка, что значение является числом в диапазоне 0-1000000
 */
public class AmountRangeHandler implements ValueHandler {
    @Override
    public Object handle(Object value) {
        try {
            double val = Double.parseDouble(value.toString());
            if (val < 0 || val > 1_000_000) {
                return "0"; // Или выбрасывать RuntimeException, если нужно прервать ОФС
            }
            return String.valueOf(val);
        } catch (Exception e) {
            return "0";
        }
    }
}