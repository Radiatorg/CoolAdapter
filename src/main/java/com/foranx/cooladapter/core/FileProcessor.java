package com.foranx.cooladapter.core;

import com.foranx.cooladapter.config.AppConfiguration;
import com.foranx.cooladapter.config.FolderConfiguration;
import com.foranx.cooladapter.parser.FileParser;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileProcessor {

    private static final Logger log = Logger.getLogger(FileProcessor.class.getName());
    private final AppConfiguration config;

    public FileProcessor(AppConfiguration config) {
        this.config = config;
    }

    public boolean processFile(Path file) {
        if (!waitForFileStability(file)) {
            log.warning(">>> File skipped or unstable (locked/empty): " + file);
            return false;
        }

        try {
            if (!Files.isRegularFile(file)) return false;

            Path parentDir = file.getParent();
            if (parentDir.getFileName().toString().equals(".processed")) return false;

            String ext = getFileExtension(file.getFileName().toString());
            if (!config.getSupportedExtensions().contains(ext) && !ext.equals("properties")) return false;

            Path propertiesFile;
            try (var stream = Files.list(parentDir)) {
                propertiesFile = stream
                        .filter(p -> p.toString().endsWith(".properties"))
                        .findFirst()
                        .orElse(null);
            }

            FolderConfiguration folderConfig = null;
            if (propertiesFile != null) {
                try {
                    folderConfig = FolderConfiguration.load(propertiesFile);
                    log.info(">>> [Local Config Loaded] " + folderConfig.toString());
                } catch (Exception e) {
                    log.log(Level.SEVERE, ">>> Invalid configuration in " + propertiesFile, e);
                    return false;
                }
            }

            Path processedDir = parentDir.resolve(".processed");
            Files.createDirectories(processedDir);

            if (!file.getFileName().toString().endsWith(".properties")) {

                log.info(">>> START PARSING: " + file.getFileName());

                try {
                    Map<String, List<Object>> parsedData = FileParser.parse(file, folderConfig);
                    printParsedData(parsedData);

                } catch (Exception e) {
                    log.log(Level.SEVERE, ">>> Parsing failed for file: " + file, e);
                }

                log.info(">>> END PARSING");

                Path processedFile = processedDir.resolve(file.getFileName());
                if (!Files.exists(processedFile) || Files.mismatch(file, processedFile) != -1) {
                    Files.copy(file, processedFile, StandardCopyOption.REPLACE_EXISTING);
                    log.info(">>> File copied to .processed: " + processedFile);
                } else {
                    log.info(">>> File already processed and unchanged: " + file);
                }
            }

            return true;
        } catch (IOException e) {
            log.log(Level.WARNING, "Error processing file: " + file, e);
            return false;
        }
    }

    private void printParsedData(Map<String, List<Object>> data) {
        log.info("============== PARSED DATA RESULT ==============");
        if (data.isEmpty()) {
            log.info("(Empty Result)");
        } else {
            data.forEach((header, values) -> {
                StringBuilder sb = new StringBuilder();
                sb.append(String.format("COLUMN [%-15s] : ", header));
                sb.append(values.toString()); // List.toString() выведет [val1, val2, ...]
                log.info(sb.toString());
            });
        }
        log.info("================================================");
    }

    private boolean waitForFileStability(Path file) {
        long lastSize = -1;
        int attempts = 0;
        int maxAttempts = 10;

        while (attempts < maxAttempts) {
            try {
                if (!Files.exists(file) || Files.isDirectory(file)) {
                    return false;
                }

                long currentSize = Files.size(file);

                if (currentSize > 0 && currentSize == lastSize) {
                    return true;
                }

                lastSize = currentSize;
                attempts++;

                TimeUnit.MILLISECONDS.sleep(500);

            } catch (IOException | InterruptedException e) {
                log.warning("Wait interrupted or IO error: " + e.getMessage());
                return false;
            }
        }

        log.warning("Timeout waiting for file stability: " + file);
        return false;
    }


    private String getFileExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx > 0 ? filename.substring(idx + 1).toLowerCase() : "";
    }
}
