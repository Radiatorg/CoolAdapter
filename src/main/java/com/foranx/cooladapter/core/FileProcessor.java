package com.foranx.cooladapter.core;

import com.foranx.cooladapter.config.AppConfiguration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileProcessor {

    private static final Logger log = Logger.getLogger(FileProcessor.class.getName());
    private final AppConfiguration config;

    public FileProcessor(AppConfiguration config) {
        this.config = config;
    }

    public boolean processFile(Path file) {
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

            if (!file.getFileName().toString().endsWith(".properties") && propertiesFile == null) {
                log.info(">>> Skipping file " + file + ": no .properties in folder");
                return false;
            }

            if (propertiesFile != null) {
                Properties props = new Properties();
                try (InputStream in = Files.newInputStream(propertiesFile)) {
                    props.load(in);
                }
                log.info(">>> Properties from " + propertiesFile + ": " + props);
            }

            Path processedDir = parentDir.resolve(".processed");
            Files.createDirectories(processedDir);

            if (!file.getFileName().toString().endsWith(".properties")) {
                Path processedFile = processedDir.resolve(file.getFileName());
                if (!Files.exists(processedFile) || Files.mismatch(file, processedFile) != -1) {
                    log.info(">>> Processing file: " + file);
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


    private String getFileExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx > 0 ? filename.substring(idx + 1).toLowerCase() : "";
    }
}
