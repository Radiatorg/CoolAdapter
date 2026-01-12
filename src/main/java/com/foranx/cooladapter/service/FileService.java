package com.foranx.cooladapter.service;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.config.ConfigCache;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FileService {

    private static final Logger log = Logger.getLogger(FileService.class.getName());
    private static final String PROCESSED_DIR = ".processed";
    private static final String ERROR_DIR = ".error";
    private static final String CONFIG_EXT = ".properties";

    private final AppConfig appConfig;
    private final ActiveMqService mqService; // Добавили сервис
    private final Map<Path, ConfigCache> configCache = new ConcurrentHashMap<>();

    private static final java.time.format.DateTimeFormatter FILE_TS_FORMAT =
            java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss");

    public FileService(AppConfig appConfig) {
        this.appConfig = appConfig;
        this.mqService = new ActiveMqService(appConfig); // Инициализация пула
    }

    public void close() {
        mqService.close();
    }

    public void processFile(Path file) {
        if (!shouldProcess(file)) return;
        log.info(">>> START PROCESSING: " + file.getFileName());

        try {
            Path processedDir = file.getParent().resolve(PROCESSED_DIR);

            // --- НОВАЯ ЛОГИКА ПРОВЕРКИ ДУБЛЕЙ ---
            if (appConfig.checkHashBeforeCopy() && Files.exists(processedDir)) {
                // Ищем дубликат среди переименованных файлов
                if (findDuplicateInProcessed(file, processedDir)) {
                    log.warning("Duplicate file detected (hash match with archived file). Moving to .processed as duplicate.");
                    // Перемещаем, чтобы убрать из входящих (с новым таймстемпом)
                    moveFileWithTimestamp(file, processedDir, true);
                    return;
                }
            }
            // -------------------------------------
        } catch (Exception e) {
            log.warning("Warning checking backup: " + e.getMessage());
        }


        try {
            // 2. ЗАГРУЗКА КОНФИГА
            FolderConfig folderConfig = loadFolderConfigCached(file.getParent());
            if (folderConfig == null) {
                throw new IllegalStateException("Missing config file for folder: " + file.getParent());
            }

            // 3. ВАЛИДАЦИЯ (DRY RUN) - Холостой прогон
            log.info("Phase 1: Validating structure (Dry Run)...");
            ParserService.validateStructure(file, folderConfig);
            // ... валидация билдера ... (оставляем как было)
            log.info("Validation OK.");

            // 4. ОТПРАВКА (REAL RUN) - ТЕПЕРЬ ТРАНЗАКЦИОННАЯ
            log.info("Phase 2: Sending to ActiveMQ (Transactional)...");
            var sendingBuilder = new OfsMessageBuilder(folderConfig, appConfig);

            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            // ВОТ ГЛАВНОЕ ИЗМЕНЕНИЕ:
            // Мы передаем логику парсинга ВНУТРЬ транзакции ActiveMQ
            // !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
            mqService.sendBatch(sender -> {

                // Внутри этой лямбды JMS сессия открыта.
                // Если здесь вылетит Exception, ActiveMqService сделает rollback.

                ParserService.parseStream(file, folderConfig, rowData -> {
                    String ofsMessage = sendingBuilder.buildSingleMessage(rowData);
                    if (ofsMessage != null) {
                        try {
                            // Отправляем через транзакционный sender
                            sender.send(ofsMessage);
                        } catch (Exception e) {
                            // Оборачиваем в Runtime, чтобы прервать parseStream и стриггерить rollback
                            throw new RuntimeException("Error sending JMS message", e);
                        }
                    }
                });

                log.info("All messages sent to producer buffer. Waiting for commit...");
            });

            // 5. УСПЕХ -> Если мы здесь, значит commit прошел
            moveToProcessed(file);
            log.info(">>> SUCCESS: File moved to .processed: " + file.getFileName());

        } catch (Exception e) {
            // 6. ОШИБКА -> Rollback уже произошел внутри mqService
            log.log(Level.SEVERE, "Error processing file (Transaction Rolled Back): " + file, e);
            moveToError(file, e);
        }
    }

    private boolean findDuplicateInProcessed(Path incomingFile, Path processedDir) {
        String originalName = incomingFile.getFileName().toString();
        String nameWithoutExt = getNameWithoutExtension(originalName);
        String ext = getExtension(originalName); // например ".csv"

        try (var stream = Files.list(processedDir)) {
            // 1. Фильтруем файлы: они должны начинаться так же и иметь такое же расширение
            List<Path> candidates = stream
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String pName = path.getFileName().toString();
                        // Проверка: файл начинается с "name_" и заканчивается на ".csv"
                        // Это отсечет "orders_v2.csv", если ищем "orders.csv"
                        return pName.startsWith(nameWithoutExt + "_") && pName.endsWith(ext);
                    })
                    .toList(); // Собираем в список, чтобы не держать стрим открытым при чтении хешей

            // 2. Проверяем хеш у кандидатов
            for (Path candidate : candidates) {
                if (isSameContent(incomingFile, candidate)) {
                    log.info("Found duplicate content in: " + candidate.getFileName());
                    return true;
                }
            }
        } catch (IOException e) {
            log.warning("Failed to scan .processed for duplicates: " + e.getMessage());
        }
        return false;
    }

    private String getNameWithoutExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? fileName : fileName.substring(0, dotIndex);
    }

    private String getExtension(String fileName) {
        int dotIndex = fileName.lastIndexOf('.');
        return (dotIndex == -1) ? "" : fileName.substring(dotIndex);
    }

    private void moveToProcessed(Path source) throws IOException {
        Path targetDir = source.getParent().resolve(PROCESSED_DIR);
        // Передаем true, так как это успех
        moveFileWithTimestamp(source, targetDir, true);
    }


    private FolderConfig loadFolderConfigCached(Path folder) throws IOException {
        // Ищем файл свойств
        List<Path> propFiles;
        try (var stream = Files.list(folder)) {
            propFiles = stream.filter(p -> p.toString().endsWith(CONFIG_EXT)).toList();
        }
        if (propFiles.isEmpty()) return null;

        Path configFile = propFiles.get(0); // Берем первый попавшийся

        // --- ИСПРАВЛЕНИЕ ТУТ ---
        // Получаем время изменения файла напрямую
        java.nio.file.attribute.FileTime currentModified = Files.getLastModifiedTime(configFile);

        // Проверяем кэш
        ConfigCache cached = configCache.get(configFile);

        if (cached != null && cached.lastModified().equals(currentModified)) {
            // Файл не менялся, отдаем из памяти
            return cached.config();
        }

        // Файл изменился или его нет в кэше -> Читаем с диска
        log.info("Reloading config from disk: " + configFile);
        Properties props = new Properties();
        try (var reader = new InputStreamReader(Files.newInputStream(configFile), StandardCharsets.UTF_8)) {
            props.load(reader);
        }

        FolderConfig newConfig = FolderConfig.fromProperties(props, folder.getFileName().toString());

        // Обновляем кэш
        configCache.put(configFile, new ConfigCache(newConfig, currentModified));

        return newConfig;
    }

    private void moveToError(Path source, Exception reason) {
        try {
            Path targetDir = source.getParent().resolve(ERROR_DIR);

            // 1. Перемещаем файл с таймстемпом
            Path targetFile = moveFileWithTimestamp(source, targetDir, false);

            // 2. Создаем отчет об ошибке с ТЕМ ЖЕ именем, что и новый файл + .err.txt
            // Пример: input.csv -> input_20231025_120000.csv.err.txt
            Path errorReport = targetDir.resolve(targetFile.getFileName() + ".err.txt");

            String errorMsg = "Time: " + java.time.LocalDateTime.now() + "\n" +
                    "Original File: " + source.getFileName() + "\n" +
                    "Error: " + reason.getMessage() + "\n" +
                    "Stacktrace:\n" + getStackTraceAsString(reason);

            Files.writeString(errorReport, errorMsg,
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);

        } catch (IOException e) {
            log.severe("CRITICAL: Could not move file to .error folder! File: " + source);
            e.printStackTrace();
        }
    }

    private Path moveFileWithTimestamp(Path source, Path targetDir, boolean isSuccess) throws IOException {
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }

        String originalName = source.getFileName().toString();
        String nameWithoutExt = originalName;
        String ext = "";

        int dotIndex = originalName.lastIndexOf('.');
        if (dotIndex != -1) {
            nameWithoutExt = originalName.substring(0, dotIndex);
            ext = originalName.substring(dotIndex); // например ".csv"
        }

        // Формируем уникальное имя: Name_YYYYMMDD_HHMMSS.ext
        String timestamp = java.time.LocalDateTime.now().format(FILE_TS_FORMAT);
        String newName = nameWithoutExt + "_" + timestamp + ext;

        Path target = targetDir.resolve(newName);

        // Используем ATOMIC_MOVE, если возможно.
        // REPLACE_EXISTING здесь формально не нужен (имя уникально), но оставим для надежности.
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // Фолбек для разных файловых систем
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }

        return target;
    }

    private Path moveFile(Path source, Path targetDir) throws IOException {
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }
        Path target = targetDir.resolve(source.getFileName());

        // ATOMIC_MOVE + REPLACE_EXISTING гарантирует целостность
        try {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (IOException e) {
            // Если ATOMIC_MOVE не поддерживается файловой системой (например между разными дисками), пробуем просто переместить
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
        if (name.endsWith(CONFIG_EXT)) return false;
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

    private void copyToProcessed(Path source) throws IOException {
        copyToDir(source, PROCESSED_DIR);
    }

    private void copyToDir(Path source, String dirName) throws IOException {
        Path targetDir = source.getParent().resolve(dirName);
        if (!Files.exists(targetDir)) {
            Files.createDirectories(targetDir);
        }
        Path target = targetDir.resolve(source.getFileName());
        Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
    }

    private boolean isSameContent(Path file1, Path file2) {
        try {
            // 1. Сначала дешевая проверка размера
            if (Files.size(file1) != Files.size(file2)) return false;

            // 2. Только если размер совпал - считаем тяжелый хеш
            String hash1 = calculateFileHash(file1);
            String hash2 = calculateFileHash(file2);
            return hash1.equals(hash2);
        } catch (Exception e) {
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
}