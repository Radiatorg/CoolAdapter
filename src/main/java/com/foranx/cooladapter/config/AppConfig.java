package com.foranx.cooladapter.config;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public record AppConfig(
        List<String> supportedExtensions,
        String logFolder,
        String fallbackLogName,
        Credentials credentials,
        Level logLevel,
        Path directory,
        URI activeMqUrl,
        String queue,
        boolean checkHashBeforeCopy,
        long fileStabilityThreshold,
        long maxFileWaitTime
) {
    private static final Logger log = Logger.getLogger(AppConfig.class.getName());

    public void logConfiguration() {
        log.info(() -> """
        === Application configuration loaded ===
        supportedExtensions = %s
        logFolder           = %s
        directory           = %s
        activeMqUrl         = %s
        credentials         = %s
        checkHashBeforeCopy = %s
        ========================================
        """.formatted(
                supportedExtensions,
                logFolder,
                directory,
                activeMqUrl,
                credentials,
                checkHashBeforeCopy
        ));
    }
}