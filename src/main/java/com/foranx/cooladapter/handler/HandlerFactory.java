package com.foranx.cooladapter.handler;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public class HandlerFactory {
    private static final Logger log = Logger.getLogger(HandlerFactory.class.getName());
    private static final Map<String, ValueHandler> cache = new ConcurrentHashMap<>();

    public static ValueHandler getHandler(String className, String jarPath) {
        if (className == null || className.isBlank() || className.endsWith(".java")) {
            return null;
        }

        String cacheKey = (jarPath == null || jarPath.isBlank())
                ? className
                : className + "::" + jarPath;

        return cache.computeIfAbsent(cacheKey, k -> createHandlerInstance(className, jarPath));
    }

    private static ValueHandler createHandlerInstance(String className, String jarPath) {
        try {
            Class<?> clazz;

            if (jarPath != null && !jarPath.isBlank()) {
                Path path = Paths.get(jarPath);
                if (!path.toFile().exists()) {
                    log.warning("Handler JAR not found at: " + jarPath + ". Fallback to classpath.");
                    clazz = Class.forName(className);
                } else {
                    log.info("Loading handler [" + className + "] from JAR: " + jarPath);
                    URL url = path.toUri().toURL();
                    URLClassLoader loader = new URLClassLoader(new URL[]{url}, HandlerFactory.class.getClassLoader());
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
        } catch (MalformedURLException e) {
            log.warning("Invalid JAR path: " + jarPath);
            return null;
        } catch (Exception e) {
            log.warning("Could not instantiate handler [" + className + "]: " + e.getMessage());
            return null;
        }
    }
}