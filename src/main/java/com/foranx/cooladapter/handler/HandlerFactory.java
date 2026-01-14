package com.foranx.cooladapter.handler;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HandlerFactory {
    private static final Logger log = Logger.getLogger(HandlerFactory.class.getName());

    private static final Map<String, ValueHandler> handlerCache = new ConcurrentHashMap<>();
    private static final Map<String, URLClassLoader> loaderCache = new ConcurrentHashMap<>();

    public static ValueHandler getHandler(String className, String jarPath) {
        if (className == null || className.isBlank() || className.endsWith(".java")) {
            return null;
        }

        String cacheKey = (jarPath == null || jarPath.isBlank())
                ? className
                : className + "::" + jarPath;

        return handlerCache.computeIfAbsent(cacheKey, k -> createHandlerInstance(className, jarPath));
    }

    private static ValueHandler createHandlerInstance(String className, String jarPath) {
        try {
            Class<?> clazz;

            if (jarPath != null && !jarPath.isBlank()) {
                URLClassLoader loader = getOrCreateClassLoader(jarPath);
                if (loader == null) {
                    clazz = Class.forName(className);
                } else {
                    log.info("Loading handler [" + className + "] from JAR: " + jarPath);
                    clazz = loader.loadClass(className);
                }
            } else {
                log.info("Loading handler class via reflection (classpath): " + className);
                clazz = Class.forName(className);
            }

            if (!ValueHandler.class.isAssignableFrom(clazz)) {
                log.warning("Class " + className + " does not implement ValueHandler interface");
                return null;
            }

            return (ValueHandler) clazz.getDeclaredConstructor().newInstance();

        } catch (ClassNotFoundException e) {
            log.warning("Class not found: " + className + " (Path: " + jarPath + ")");
            return null;
        } catch (Exception e) {
            log.log(Level.WARNING, "Could not instantiate handler [" + className + "]", e);
            return null;
        }
    }

    private static synchronized URLClassLoader getOrCreateClassLoader(String jarPath) {
        if (loaderCache.containsKey(jarPath)) {
            return loaderCache.get(jarPath);
        }

        Path path = Paths.get(jarPath);
        if (!path.toFile().exists()) {
            log.warning("Handler JAR not found at: " + jarPath + ". Fallback to classpath.");
            return null;
        }

        try {
            URL url = path.toUri().toURL();
            URLClassLoader loader = new URLClassLoader(new URL[]{url}, HandlerFactory.class.getClassLoader());
            loaderCache.put(jarPath, loader);
            return loader;
        } catch (MalformedURLException e) {
            log.warning("Invalid JAR path URL: " + jarPath);
            return null;
        }
    }

    public static synchronized void closeAll() {
        handlerCache.clear();
        for (Map.Entry<String, URLClassLoader> entry : loaderCache.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.warning("Error closing ClassLoader for: " + entry.getKey());
            }
        }
        loaderCache.clear();
        log.info("HandlerFactory resources closed.");
    }
}