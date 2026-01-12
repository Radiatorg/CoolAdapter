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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class FileService {

    private static final Logger log = Logger.getLogger(FileService.class.getName());
    private static final String PROCESSED_DIR = ".processed";
    private static final String ERROR_DIR = ".error"; // Папка для битых файлов
    private static final String CONFIG_EXT = ".properties";

    private final AppConfig appConfig;

    public FileService(AppConfig appConfig) {
        this.appConfig = appConfig;
    }

    public void processFile(Path file) {
        if (!shouldProcess(file)) return;

        // 1. ЗАЩИТА ОТ ПОВТОРНОЙ ОБРАБОТКИ
        // Так как мы НЕ удаляем файл, при рестарте он снова найдется.
        // Проверяем: если он уже лежит в .processed и контент совпадает -> пропускаем.
        try {
            Path targetDir = file.getParent().resolve(PROCESSED_DIR);
            Path targetFile = targetDir.resolve(file.getFileName());

            if (Files.exists(targetFile)) {
                if (appConfig.checkHashBeforeCopy() && isSameContent(file, targetFile)) {
                    log.info(">>> SKIPPING: File already processed (same content found in backup): " + file.getFileName());
                    return; // ВЫХОДИМ, НЕ ОБРАБАТЫВАЕМ
                }
            }
        } catch (Exception e) {
            log.warning("Warning: Could not check existing backup, proceeding with processing: " + e.getMessage());
        }

        log.info(">>> START PROCESSING: " + file.getFileName());

        try {
            FolderConfig folderConfig = loadFolderConfig(file.getParent());
            if (folderConfig == null) {
                log.warning("No config found for: " + file);
                // Если конфига нет, просто копируем в обработанное, чтобы не висел вечно?
                copyToProcessed(file);
                return;
            }

            // 2. БИЗНЕС ЛОГИКА
            var data = ParserService.parse(file, folderConfig);
            var builder = new OfsMessageBuilder(folderConfig, appConfig);
            var messages = builder.build(data);

            logOfsMessages(messages);
            // sendToActiveMq(messages);

            // 3. КОПИРОВАНИЕ В BACKUP (Без удаления оригинала!)
            copyToProcessed(file);

            log.info(">>> END PROCESSING: " + file.getFileName());

        } catch (Exception e) {
            log.log(Level.SEVERE, "Error processing file: " + file, e);
            // Если ошибка - копируем в .error, оригинал тоже оставляем
            try {
                copyToDir(file, ERROR_DIR);
            } catch (IOException ex) {
                log.warning("Failed to copy to .error: " + ex.getMessage());
            }
        }
    }

    // Метод копирует файл в .processed с ОРИГИНАЛЬНЫМ именем
    private void copyToProcessed(Path source) throws IOException {
        copyToDir(source, PROCESSED_DIR);
    }

    private void copyToDir(Path source, String dirName) throws IOException {
        Path targetDir = source.getParent().resolve(dirName);
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }

        // Имя берем 1-в-1 как у источника. Никаких таймстампов.
        Path target = targetDir.resolve(source.getFileName());

        // REPLACE_EXISTING обновит бэкап, если файл изменился
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);

        log.info(">>> File COPIED to " + dirName + ": " + target);
    }

    // --- Вспомогательные методы проверки хэша (как у тебя были) ---

    private boolean isSameContent(Path file1, Path file2) {
        try {
            String hash1 = calculateFileHash(file1);
            String hash2 = calculateFileHash(file2);
            return hash1.equals(hash2);
        } catch (Exception e) {
            log.log(Level.WARNING, "Hash calculation failed", e);
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

    // ... loadFolderConfig, shouldProcess, logOfsMessages оставляем как есть ...
    // ... waitForStability НЕ НУЖЕН (он в мониторе) ...
    // ... (Остальные методы скопируй из своего старого файла) ...

    // ВАЖНО: Ниже добавь недостающие методы, чтобы код компилировался
    // (loadFolderConfig, shouldProcess, logOfsMessages - они были в твоем коде)

    private FolderConfig loadFolderConfig(Path folder) throws IOException {
        // ... (Твой код) ...
        List<Path> propFiles;
        try (var stream = Files.list(folder)) {
            propFiles = stream.filter(p -> p.toString().endsWith(CONFIG_EXT)).toList();
        }
        if (propFiles.isEmpty()) return null;
        if (propFiles.size() > 1) throw new IllegalStateException("Ambiguous config");

        Path propFile = propFiles.get(0);
        Properties props = new Properties();
        try (var reader = new InputStreamReader(Files.newInputStream(propFile), StandardCharsets.UTF_8)) {
            props.load(reader);
        }
        return FolderConfig.fromProperties(props, folder.getFileName().toString());
    }

    private boolean shouldProcess(Path file) {
        if (!Files.isRegularFile(file)) return false;
        String name = file.getFileName().toString();
        // Игнорируем конфиги и скрытые папки (.processed, .error)
        if (name.endsWith(CONFIG_EXT)) return false;

        // Доп. защита: не обрабатывать файлы внутри .processed, если Watcher настроен рекурсивно
        if (file.toString().contains(PROCESSED_DIR) || file.toString().contains(ERROR_DIR)) return false;

        int lastDot = name.lastIndexOf('.');
        if (lastDot == -1) return false;
        String ext = name.substring(lastDot + 1).toLowerCase();
        return appConfig.supportedExtensions().contains(ext);
    }

    private void logOfsMessages(List<String> messages) {
        log.info("============== OFS MESSAGES ==============");
        messages.forEach(log::info);
        log.info("==========================================");
    }
}