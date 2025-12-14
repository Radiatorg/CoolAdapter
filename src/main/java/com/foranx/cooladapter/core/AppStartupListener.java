package com.foranx.cooladapter.core;

import com.foranx.cooladapter.DirectoryWatcher;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

@WebListener
public class AppStartupListener implements ServletContextListener {

    private DirectoryWatcher watcher;

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
            watcher.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
