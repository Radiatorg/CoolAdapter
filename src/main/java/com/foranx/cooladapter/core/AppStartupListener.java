package com.foranx.cooladapter.core;

import com.foranx.cooladapter.config.AppConfiguration;
import com.foranx.cooladapter.config.LoggingConfiguration;
import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebListener
public class AppStartupListener implements ServletContextListener {

    private DirectoryWatcher watcher;
    private static final Logger log = Logger.getLogger(AppStartupListener.class.getName());
    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            InputStream in = sce.getServletContext()
                    .getResourceAsStream("/WEB-INF/classes/application.properties");

            AppConfiguration config = new AppConfiguration();
            config.init(in);
            LoggingConfiguration.init(config.getLogFolder(), config.getLogLevel());
            config.logConfiguration();

            watcher = new DirectoryWatcher(config);
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
