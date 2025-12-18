package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.FolderConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileService {

    private static final Logger log = Logger.getLogger(FileService.class.getName());
    private static final String PROCESSED_DIR = ".processed";
    private static final String CONFIG_EXT = ".properties";

    private final AppConfig appConfig;

    public FileService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public void processFile(Path file) {
        if (!shouldProcess(file)) {
            return;
        }

        if (!waitForStability(file)) {
            log.warning(">>> File skipped or unstable (locked/empty): " + file);
            return;
        }

        try {
            log.info(">>> START PROCESSING: " + file.getFileName());

            FolderConfig folderConfig = loadFolderConfig(file.getParent());

            if (folderConfig != null) {
                try {
                    Map<String, List<Object>> data = ParserService.parse(file, folderConfig);
                    printParsedData(data);
                } catch (Exception e) {
                    log.log(Level.SEVERE, ">>> Parsing failed for file: " + file, e);
                    // Решаем, прерывать ли обработку или все равно перемещать файл
                }
            } else {
                log.warning(">>> No local .properties found for folder: " + file.getParent() + ". Skipping parsing.");
            }

            moveToProcessed(file);

            log.info(">>> END PROCESSING: " + file.getFileName());

        } catch (Exception e) {
            log.log(Level.SEVERE, "Error processing file: " + file, e);
        }
    }

    private boolean shouldProcess(Path file) {
        if (!Files.isRegularFile(file)) return false;

        String filename = file.getFileName().toString();
        if (filename.endsWith(CONFIG_EXT)) return false;
        if (file.getParent().endsWith(PROCESSED_DIR)) return false;

        String ext = getExtension(filename);
        return appConfig.getSupportedExtensions().contains(ext);
    }

    private FolderConfig loadFolderConfig(Path folder) {
        try (var stream = Files.list(folder)) {
            Path propertiesFile = stream
                    .filter(p -> p.toString().endsWith(CONFIG_EXT))
                    .findFirst()
                    .orElse(null);

            if (propertiesFile != null) {
                Properties props = new Properties();
                try (InputStream in = Files.newInputStream(propertiesFile)) {
                    props.load(in);
                }
                return FolderConfig.fromProperties(props, folder.getFileName().toString());
            }
        } catch (IOException e) {
            log.warning("Failed to search/load properties in " + folder);
        }
        return null;
    }

    private void moveToProcessed(Path file) throws IOException {
        Path processedDir = file.getParent().resolve(PROCESSED_DIR);
        if (!Files.exists(processedDir)) {
            Files.createDirectories(processedDir);
        }

        Path target = processedDir.resolve(file.getFileName());

        Files.move(file, target, StandardCopyOption.REPLACE_EXISTING);
        log.info(">>> File moved to: " + target);
    }

    private boolean waitForStability(Path file) {
        long lastSize = -1;
        int attempts = 0;
        int maxAttempts = 5;

        while (attempts < maxAttempts) {
            try {
                if (!Files.exists(file)) return false;
                long currentSize = Files.size(file);
                if (currentSize > 0 && currentSize == lastSize) {
                    return true;
                }
                lastSize = currentSize;
                attempts++;
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (IOException | InterruptedException e) {
                return false;
            }
        }
        return false;
    }

    private void printParsedData(Map<String, List<Object>> data) {
        log.info("============== PARSED DATA ==============");
        if (data.isEmpty()) {
            log.info("(Empty Result)");
        } else {
            data.forEach((header, values) ->
                    log.info(String.format("COLUMN [%-15s] : %s", header, values))
            );
        }
        log.info("=========================================");
    }

    private String getExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx > 0 ? filename.substring(idx + 1).toLowerCase() : "";
    }
}