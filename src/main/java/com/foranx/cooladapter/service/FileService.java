package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.FolderConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class FileService {

    private static final Logger log = Logger.getLogger(FileService.class.getName());
    private static final String PROCESSED_DIR = ".processed";
    private static final String CONFIG_EXT = ".properties";

    private final AppConfig appConfig;

    public FileService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public void processFile(Path file) {
        if (!shouldProcess(file)) return;

        if (!waitForStability(file)) {
            log.warning(">>> File skipped (unstable/locked): " + file);
            return;
        }

        log.info(">>> START PROCESSING: " + file.getFileName());

        try {
            FolderConfig folderConfig = loadFolderConfig(file.getParent());
            if (folderConfig == null) {
                log.warning(">>> Parsing skipped: No configuration found for " + file.getParent());
                copyToProcessed(file);
                return;
            }

            Map<String, List<Object>> data = ParserService.parse(file, folderConfig);

            OfsMessageBuilder builder = new OfsMessageBuilder(folderConfig, appConfig);
            List<String> ofsMessages = builder.build(data);

            logOfsMessages(ofsMessages);

            copyToProcessed(file);
            log.info(">>> END PROCESSING: " + file.getFileName());

        } catch (Exception e) {
            log.log(Level.SEVERE, "Error processing file: " + file, e);
        }
    }

    private void logOfsMessages(List<String> messages) {
        log.info("============== OFS MESSAGES FOR T24 ==============");
        if (messages.isEmpty()) {
            log.info("(No records to process)");
        } else {
            messages.forEach(log::info);
        }
        log.info("==================================================");
    }

    private FolderConfig loadFolderConfig(Path folder) throws IOException {
        List<Path> propFiles;
        try (var stream = Files.list(folder)) {
            propFiles = stream
                    .filter(p -> p.toString().endsWith(CONFIG_EXT))
                    .toList();
        }

        if (propFiles.isEmpty()) return null;

        if (propFiles.size() > 1) {
            String names = propFiles.stream().map(p -> p.getFileName().toString()).collect(Collectors.joining(", "));
            throw new IllegalStateException("Ambiguous configuration in " + folder + ": [" + names + "]");
        }

        Path propFile = propFiles.getFirst();
        Properties props = new Properties();
        try (var reader = new InputStreamReader(Files.newInputStream(propFile), StandardCharsets.UTF_8)) {
            props.load(reader);
        }

        return FolderConfig.fromProperties(props, folder.getFileName().toString());
    }

    private void copyToProcessed(Path source) throws IOException {
        Path targetDir = source.getParent().resolve(PROCESSED_DIR);
        Files.createDirectories(targetDir);
        Path target = targetDir.resolve(source.getFileName());

        if (appConfig.checkHashBeforeCopy() && Files.exists(target)) {
            if (isSameContent(source, target)) {
                log.info(">>> File exists in .processed with SAME CONTENT. Skipping copy.");
                return;
            }
            log.warning(">>> File in .processed differs. Overwriting.");
        }

        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        log.info(">>> File COPIED to: " + target);
    }

    private boolean isSameContent(Path file1, Path file2) {
        try {
            String hash1 = calculateFileHash(file1);
            String hash2 = calculateFileHash(file2);
            return hash1.equals(hash2);
        } catch (Exception e) {
            log.log(Level.WARNING, "Hash calculation failed, assuming files differ", e);
            return false;
        }
    }

    private String calculateFileHash(Path path) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int n;
            while ((n = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, n);
            }
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : digest.digest()) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private boolean shouldProcess(Path file) {
        if (!Files.isRegularFile(file)) return false;
        String name = file.getFileName().toString();
        if (name.endsWith(CONFIG_EXT) || file.getParent().endsWith(PROCESSED_DIR)) return false;

        int lastDot = name.lastIndexOf('.');
        if (lastDot == -1) return false;
        String ext = name.substring(lastDot + 1).toLowerCase();

        return appConfig.supportedExtensions().contains(ext);
    }

    private boolean waitForStability(Path file) {
        final long SILENCE_TIMEOUT = 5_000;
        final long MAX_GLOBAL_WAIT = 900_000;
        final long RETRY_DELAY = 1_000;

        long startTime = System.currentTimeMillis();
        long lastSize = -1;
        long lastChangeTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < MAX_GLOBAL_WAIT) {
            try {
                if (!Files.exists(file)) return false;

                long currentSize = Files.size(file);

                if (currentSize == 0) {
                    TimeUnit.MILLISECONDS.sleep(RETRY_DELAY);
                    continue;
                }

                long now = System.currentTimeMillis();

                if (currentSize != lastSize) {
                    lastSize = currentSize;
                    lastChangeTime = now;
                } else {
                    if ((now - lastChangeTime) > SILENCE_TIMEOUT) {
                        try (var raf = new java.io.RandomAccessFile(file.toFile(), "rw")) {
                            return true;
                        } catch (IOException e) {
                            // Файл занят другим процессом. Ждем дальше.
                        }
                    }
                }

                TimeUnit.MILLISECONDS.sleep(RETRY_DELAY);

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            } catch (IOException e) {
                // Ошибки доступа к файловой системе (например, сетевой диск отвалился)
            }
        }

        log.warning(">>> TIMEOUT: File never became stable (locked or too slow): " + file);
        return false;
    }
}