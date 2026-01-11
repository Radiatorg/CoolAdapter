package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.FolderConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

                    List<String> ofsMessages = generateOfsMessages(data, folderConfig);

                    log.info("============== OFS MESSAGES FOR T24 ==============");
                    if (ofsMessages.isEmpty()) {
                        log.info("(No records to process)");
                    } else {
                        ofsMessages.forEach(log::info);
                    }
                    log.info("==================================================");

                } catch (Exception e) {
                    log.log(Level.SEVERE, ">>> Parsing or OFS generation failed for file: " + file, e);
                }
            }
            else {
                log.warning(">>> No local .properties found for folder: " + file.getParent() + ". Skipping parsing.");
            }

            copyToProcessed(file);

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
        return appConfig.supportedExtensions().contains(ext);
    }

    private FolderConfig loadFolderConfig(Path folder) {
        List<Path> propFiles;

        try (var stream = Files.list(folder)) {
            propFiles = stream
                    .filter(p -> p.toString().endsWith(CONFIG_EXT))
                    .toList();
        } catch (IOException e) {
            log.warning("Failed to search properties in " + folder);
            return null;
        }

        if (propFiles.isEmpty()) {
            return null;
        }

        if (propFiles.size() > 1) {
            String fileNames = propFiles.stream()
                    .map(p -> p.getFileName().toString())
                    .collect(Collectors.joining(", "));

            throw new IllegalStateException(
                    String.format("Ambiguous configuration: Found %d .properties files in folder '%s': [%s]. Allowed only one.",
                            propFiles.size(), folder, fileNames)
            );
        }

        Path propertiesFile = propFiles.getFirst();
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(propertiesFile)) {
            props.load(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties file: " + propertiesFile, e);
        }

        return FolderConfig.fromProperties(props, folder.getFileName().toString());
    }

    private void copyToProcessed(Path file) throws IOException {
        Path processedDir = file.getParent().resolve(PROCESSED_DIR);
        if (!Files.exists(processedDir)) {
            Files.createDirectories(processedDir);
        }

        Path target = processedDir.resolve(file.getFileName());

        if (appConfig.checkHashBeforeCopy() && Files.exists(target)) {
            try {
                String sourceHash = calculateFileHash(file);
                String targetHash = calculateFileHash(target);

                if (sourceHash.equals(targetHash)) {
                    log.info(">>> File already exists in .processed with SAME CONTENT (Hash match). Skipping copy.");
                    return;
                } else {
                    log.warning(">>> File in .processed differs from source. Updating backup copy.");
                }
            } catch (NoSuchAlgorithmException e) {
                log.log(Level.SEVERE, "Hashing algorithm error", e);
            }
        }

        Files.copy(file, target, StandardCopyOption.REPLACE_EXISTING);
        log.info(">>> File COPIED to: " + target);
    }

    private String calculateFileHash(Path path) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = Files.newInputStream(path)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        byte[] hashBytes = digest.digest();

        StringBuilder sb = new StringBuilder();
        for (byte b : hashBytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
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

    private List<String> generateOfsMessages(Map<String, List<Object>> data, FolderConfig folderConfig) {
        List<String> ofsMessages = new ArrayList<>();

        if (data.isEmpty()) {
            return ofsMessages;
        }

        String operation = folderConfig.getTableVersion();
        String options = folderConfig.getTableVersion() + "/I/PROCESS";
        String userInformation = appConfig.credentials().username();

        List<String> headers = new ArrayList<>(data.keySet());
        if (headers.isEmpty()) return ofsMessages;

        int recordCount = data.get(headers.get(0)).size();
        String idHeader = headers.get(0);

        for (int i = 0; i < recordCount; i++) {
            String idInformation = data.get(idHeader).get(i).toString();

            StringBuilder dataPart = new StringBuilder();
            for (int h = 1; h < headers.size(); h++) {
                String header = headers.get(h);
                Object value = data.get(header).get(i);

                if (value instanceof List) {
                    List<?> outerList = (List<?>) value;
                    if (!outerList.isEmpty() && outerList.get(0) instanceof List) {
                        @SuppressWarnings("unchecked")
                        List<List<String>> multiValueList = (List<List<String>>) value;
                        for (int m = 0; m < multiValueList.size(); m++) {
                            List<String> subValueList = multiValueList.get(m);
                            for (int s = 0; s < subValueList.size(); s++) {
                                dataPart.append(header).append(":").append(m + 1).append(":").append(s + 1)
                                        .append("=").append(subValueList.get(s)).append(",");
                            }
                        }
                    } else {
                        @SuppressWarnings("unchecked")
                        List<String> subValueList = (List<String>) value;
                        for (int s = 0; s < subValueList.size(); s++) {
                            dataPart.append(header).append(":").append(s + 1)
                                    .append("=").append(subValueList.get(s)).append(",");
                        }
                    }
                } else {
                    dataPart.append(header).append("=").append(value).append(",");
                }
            }

            if (dataPart.length() > 0) {
                dataPart.deleteCharAt(dataPart.length() - 1);
            }

            String finalOfsMessage = String.join(",", operation, options, userInformation, idInformation, dataPart.toString());
            ofsMessages.add(finalOfsMessage);
        }

        return ofsMessages;
    }

    private String getExtension(String filename) {
        int idx = filename.lastIndexOf('.');
        return idx > 0 ? filename.substring(idx + 1).toLowerCase() : "";
    }
}