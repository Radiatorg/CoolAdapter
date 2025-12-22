package com.foranx.cooladapter.handler;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class HandlerFactory {
    private static final Logger log = Logger.getLogger(HandlerFactory.class.getName());
    private static final Map<String, ValueHandler> cache = new ConcurrentHashMap<>();

    public static ValueHandler getHandler(String className) {
        if (className == null || className.isBlank() || className.endsWith(".java")) {
            return null;
        }

        return cache.computeIfAbsent(className, name -> {
            try {
                log.info("Loading handler class via reflection: " + name);
                Class<?> clazz = Class.forName(name);
                return (ValueHandler) clazz.getDeclaredConstructor().newInstance();
            } catch (Exception e) {
                log.warning("Could not instantiate handler [" + name + "]: " + e.getMessage());
                return null;
            }
        });
    }
}