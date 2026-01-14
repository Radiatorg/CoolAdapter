package com.foranx.cooladapter.web;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.AppConfigLoader;
import com.foranx.cooladapter.service.FileService;
import com.foranx.cooladapter.util.LoggingUtil;
import com.foranx.cooladapter.watcher.DirectoryWatcher;
import com.foranx.cooladapter.watcher.FileStabilityMonitor;

import jakarta.servlet.ServletContextEvent;
import jakarta.servlet.ServletContextListener;
import jakarta.servlet.annotation.WebListener;

import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebListener
public class AppStartupListener implements ServletContextListener {

    private static final Logger log = Logger.getLogger(AppStartupListener.class.getName());

    private DirectoryWatcher watcher;
    private FileStabilityMonitor monitor;
    private ExecutorService processingPool;
    private FileService fileService;

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        try {
            System.out.println("Initializing CoolAdapter...");

            InputStream in = sce.getServletContext()
                    .getResourceAsStream("/WEB-INF/classes/application.properties");

            AppConfig config = AppConfigLoader.load(in);

            LoggingUtil.configure(config.logFolder(), config.logLevel().getName());
            config.logConfiguration();

            this.fileService = new FileService(config);

            processingPool = Executors.newFixedThreadPool(10);
            monitor = new FileStabilityMonitor(config, fileService, processingPool);
            monitor.start();

            watcher = new DirectoryWatcher(config, monitor);
            watcher.start();

            log.info("CoolAdapter started successfully.");

        } catch (Exception e) {
            log.log(Level.SEVERE, "CRITICAL: Failed to start application", e);
            throw new RuntimeException("Failed to start application", e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        log.info("Stopping CoolAdapter...");

        if (watcher != null) watcher.stop();
        if (monitor != null) monitor.stop();

        if (processingPool != null) {
            processingPool.shutdownNow();
        }

        if (fileService != null) {
            fileService.close();
            log.info("FileService (ActiveMQ connections) closed.");
        }

        com.foranx.cooladapter.handler.HandlerFactory.closeAll();
        LoggingUtil.closeHandlers();
    }
}