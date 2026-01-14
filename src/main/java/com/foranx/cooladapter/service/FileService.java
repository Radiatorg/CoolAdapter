package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.ConfigCache;
import com.foranx.cooladapter.config.FolderConfig;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileService {

    private static final Logger log = Logger.getLogger(FileService.class.getName());

    private static final String PROCESSED_DIR = ".processed";
    private static final String ERROR_DIR = ".error";
    private static final String CONFIG_EXT = ".properties";
    private static final String HASH_EXT = ".sha256";
    private static final String HASH_INDEX_FILE = "processed_hashes.idx";

    private static final java.time.format.DateTimeFormatter FILE_TS_FORMAT =
            java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    private final AppConfig appConfig;
    private final ActiveMqService mqService;
    private final Map<Path, ConfigCache> configCache = new ConcurrentHashMap<>();

    private final Set<String> processedHashes = ConcurrentHashMap.newKeySet();
    private final Path hashIndexPath;

    public FileService(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.mqService = new ActiveMqService(appConfig);

        this.hashIndexPath = appConfig.directory().resolve(HASH_INDEX_FILE);
        loadHashIndex();
    }

    private void loadHashIndex() {
        if (!Files.exists(hashIndexPath)) return;

        try (var lines = Files.lines(hashIndexPath, StandardCharsets.UTF_8)) {
            lines.map(String::trim)
                    .filter(line -> !line.isEmpty())
                    .forEach(processedHashes::add);
            log.info("Loaded " + processedHashes.size() + " hashes from index.");
        } catch (IOException e) {
            log.log(Level.SEVERE, "Failed to load hash index file: " + hashIndexPath, e);
        }
    }

    private synchronized void appendToHashIndex(String hash) {
        if (processedHashes.contains(hash)) return;

        try (BufferedWriter writer = Files.newBufferedWriter(
                hashIndexPath, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            writer.write(hash);
            writer.newLine();
            processedHashes.add(hash); // Добавляем в память
        } catch (IOException e) {
            log.warning("Failed to append hash to index: " + e.getMessage());
        }
    }

    public void close() {
        mqService.close();
    }

    public void processFile(Path file) {
        if (!shouldProcess(file)) return;

        long startTime = System.currentTimeMillis();
        String fileName = file.getFileName().toString();

        try {
            long fileSize = Files.size(file);
            log.info(String.format(">>> [START] Processing file: %s (Size: %d bytes)", fileName, fileSize));
        } catch (IOException e) {
            log.info(">>> [START] Processing file: " + fileName);
        }

        String incomingHash;
        try {
            incomingHash = calculateFileHash(file);
        } catch (Exception e) {
            log.severe("Failed to calculate hash for input file: " + file);
            moveToError(file, e);
            return;
        }

        try {
            if (appConfig.checkHashBeforeCopy()) {
                if (processedHashes.contains(incomingHash)) {
                    log.warning(String.format(">>> [DUPLICATE DETECTED via INDEX] File %s has duplicate hash: %s", fileName, incomingHash));
                    moveToProcessed(file, incomingHash);
                    return;
                }
            }
        } catch (Exception e) {
            log.log(Level.SEVERE, "CRITICAL: Duplicate check failed.", e);
            moveToError(file, e);
            return;
        }

        try {
            FolderConfig folderConfig = loadFolderConfigCached(file.getParent());
            if (folderConfig == null) {
                throw new IllegalStateException("Missing config file for folder: " + file.getParent());
            }

            ParserService.validateStructure(file, folderConfig);
            var sendingBuilder = new OfsMessageBuilder(folderConfig, appConfig);
            AtomicInteger messageCount = new AtomicInteger(0);
            long sendStart = System.currentTimeMillis();

            mqService.sendBatch(sender -> ParserService.parseStream(file, folderConfig, rowData -> {
                String ofsMessage = sendingBuilder.buildSingleMessage(rowData);
                if (ofsMessage != null) {
                    try {
                        sender.send(ofsMessage);
                        messageCount.incrementAndGet();
                    } catch (Exception e) {
                        throw new RuntimeException("Error sending JMS message", e);
                    }
                }
            }));

            long sendDuration = System.currentTimeMillis() - sendStart;
            log.info(String.format("JMS Transaction Committed. Sent %d messages in %d ms.", messageCount.get(), sendDuration));

            moveToProcessed(file, incomingHash);
            if (appConfig.checkHashBeforeCopy()) {
                appendToHashIndex(incomingHash);
            }

            long totalTime = System.currentTimeMillis() - startTime;
            log.info(String.format(">>> [SUCCESS] File %s processed successfully. Total time: %d ms", fileName, totalTime));

        } catch (Exception e) {
            log.log(Level.SEVERE, ">>> [ERROR] Processing failed for file: " + fileName, e);
            moveToError(file, e);
        }
    }

    private FolderConfig loadFolderConfigCached(Path folder) throws IOException {
        List<Path> propFiles;
        try (var stream = Files.list(folder)) {
            propFiles = stream.filter(p -> p.toString().endsWith(CONFIG_EXT)).toList();
        }
        if (propFiles.isEmpty()) return null;

        Path configFile = propFiles.getFirst();
        java.nio.file.attribute.FileTime currentModified = Files.getLastModifiedTime(configFile);

        ConfigCache cached = configCache.get(configFile);
        if (cached != null && cached.lastModified().equals(currentModified)) {
            return cached.config();
        }

        log.info("Loading configuration from disk: " + configFile);
        Properties props = new Properties();
        try (var reader = new InputStreamReader(Files.newInputStream(configFile), StandardCharsets.UTF_8)) {
            props.load(reader);
        }

        FolderConfig newConfig = FolderConfig.fromProperties(props, folder.getFileName().toString());
        configCache.put(configFile, new ConfigCache(newConfig, currentModified));

        return newConfig;
    }

    private void moveToProcessed(Path source, String hash) throws IOException {
        Path targetDir = source.getParent().resolve(PROCESSED_DIR);
        Path movedFile = moveFileWithTimestamp(source, targetDir);

        if (hash != null) {
            Path sidecarPath = targetDir.resolve(movedFile.getFileName().toString() + HASH_EXT);
            Files.writeString(sidecarPath, hash, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    private void moveToError(Path source, Exception reason) {
        try {
            Path targetDir = source.getParent().resolve(ERROR_DIR);
            Path targetFile = moveFileWithTimestamp(source, targetDir);
            Path errorReport = targetDir.resolve(targetFile.getFileName() + ".err.txt");

            String errorMsg = String.format("""
                    Time: %s
                    Original File: %s
                    Error Message: %s
                    Stacktrace:
                    %s
                    """,
                    java.time.LocalDateTime.now(),
                    source.getFileName(),
                    reason.getMessage(),
                    getStackTraceAsString(reason)
            );

            Files.writeString(errorReport, errorMsg,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

            log.info("File moved to error directory: " + targetFile.getFileName());

        } catch (IOException e) {
            log.severe("CRITICAL: Could not move file to .error folder! File: " + source);
        }
    }

    private Path moveFileWithTimestamp(Path source, Path targetDir) throws IOException {
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }

        String originalName = source.getFileName().toString();
        String nameWithoutExt = getNameWithoutExtension(originalName);
        String ext = getExtension(originalName);

        String timestamp = java.time.LocalDateTime.now().format(FILE_TS_FORMAT);
        String newName = nameWithoutExt + "_" + timestamp + ext;

        Path target = targetDir.resolve(newName);

        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }

        return target;
    }

    private String getStackTraceAsString(Exception e) {
        java.io.StringWriter sw = new java.io.StringWriter();
        java.io.PrintWriter pw = new java.io.PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    private boolean shouldProcess(Path file) {
        if (!Files.isRegularFile(file)) return false;
        String name = file.getFileName().toString();
        if (name.endsWith(CONFIG_EXT) || name.endsWith(HASH_EXT)) return false;
        if (name.equals(HASH_INDEX_FILE)) return false;

        for (Path part : file) {
            String partName = part.toString();
            if (partName.equals(PROCESSED_DIR) || partName.equals(ERROR_DIR)) {
                return false;
            }
        }

        int lastDot = name.lastIndexOf('.');
        if (lastDot == -1) return false;
        String ext = name.substring(lastDot + 1).toLowerCase();
        return appConfig.supportedExtensions().contains(ext);
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

    private String getNameWithoutExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
    }

    private String getExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? "" : fileName.substring(dotIndex);
    }
}