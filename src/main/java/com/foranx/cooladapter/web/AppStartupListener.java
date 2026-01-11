package com.foranx.cooladapter.web;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.AppConfigLoader;
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
            System.out.println("Initializing CoolAdapter...");

            InputStream in = sce.getServletContext()
                    .getResourceAsStream("/WEB-INF/classes/application.properties");

            AppConfig config = AppConfigLoader.load(in);

            LoggingUtil.configure(config.logFolder(), config.logLevel().getName());
            config.logConfiguration();

            FileService fileService = new FileService(config);

            watcher = new DirectoryWatcher(config, fileService::processFile);
            watcher.start();

            log.info("CoolAdapter started successfully.");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to start application", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.info("Stopping CoolAdapter...");

        if (watcher != null) {
            watcher.stop();
            log.info("DirectoryWatcher stopped.");
        }

        LoggingUtil.closeHandlers();
        System.out.println("CoolAdapter logging handlers closed.");
    }
}