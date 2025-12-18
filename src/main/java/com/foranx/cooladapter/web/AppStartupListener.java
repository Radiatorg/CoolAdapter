package com.foranx.cooladapter.web;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.service.FileService;
import com.foranx.cooladapter.util.LoggingUtil;
import com.foranx.cooladapter.watcher.DirectoryWatcher;

import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;
import java.io.InputStream;
import java.util.logging.Logger;

@WebListener
public class AppStartupListener implements ServletContextListener {

    private static final Logger log = Logger.getLogger(AppStartupListener.class.getName());
    private DirectoryWatcher watcher;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            log.info("Initializing CoolAdapter...");

            InputStream in = sce.getServletContext()
                    .getResourceAsStream("/WEB-INF/classes/application.properties");
            AppConfig config = AppConfig.load(in);

            LoggingUtil.configure(config.getLogFolder());
            config.logConfiguration();

            FileService fileService = new FileService(config);

            watcher = new DirectoryWatcher(config, fileService::processFile);
            watcher.start();

            log.info("CoolAdapter started successfully.");

        } catch (Exception e) {
            throw new RuntimeException("Failed to start application", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        if (watcher != null) {
            watcher.stop();
            log.info("DirectoryWatcher stopped.");
        }
    }
}