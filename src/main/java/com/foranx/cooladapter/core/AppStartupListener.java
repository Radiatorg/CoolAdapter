package com.foranx.cooladapter.core;

import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

import java.util.logging.Level;
import java.util.logging.Logger;

@WebListener
public class AppStartupListener implements ServletContextListener {

    private DirectoryWatcher watcher;
    private static final Logger log = Logger.getLogger(AppStartupListener.class.getName());

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            String dir = "/t24/T24/bnk/stud";
            watcher = new DirectoryWatcher(dir);
            watcher.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start DirectoryWatcher", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        try {
            if (watcher != null) {
                watcher.stop();
                log.info("DirectoryWatcher stopped successfully.");
            }
        } catch (Exception e) {
            log.log(Level.WARNING, "Failed to stop DirectoryWatcher", e);
        }
    }
}
