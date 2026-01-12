package com.foranx.cooladapter.watcher;

import com.foranx.cooladapter.config.AppConfig;
import com.foranx.cooladapter.service.FileService;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Enterprise-grade монитор.
 * Не блокирует рабочие потоки. Использует один легковесный поток
 * для проверки состояния всех входящих файлов.
 */
public class FileStabilityMonitor {

    private static final Logger log = Logger.getLogger(FileStabilityMonitor.class.getName());

    private final AppConfig config;
    private final FileService fileService;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService processingExecutor;

    // Храним состояние отслеживаемых файлов
    private final Map<Path, FileState> trackingMap = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public FileStabilityMonitor(AppConfig config, FileService fileService, ExecutorService processingExecutor) {
        this.config = config;
        this.fileService = fileService;
        this.processingExecutor = processingExecutor;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Stability-Monitor-Thread");
            t.setDaemon(true);
            return t;
        });
    }

    public void start() {
        if (running.compareAndSet(false, true)) {
            // Запускаем проверку каждую секунду
            scheduler.scheduleAtFixedRate(this::checkFiles, 1000, 1000, TimeUnit.MILLISECONDS);
            log.info("FileStabilityMonitor started.");
        }
    }

    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdownNow();
            trackingMap.clear();
        }
    }

    // Этот метод вызывает DirectoryWatcher
    public void watch(Path file) {
        if (!running.get()) return;
        trackingMap.computeIfAbsent(file, k -> new FileState(System.currentTimeMillis()));
    }

    private void checkFiles() {
        if (trackingMap.isEmpty()) return;

        long now = System.currentTimeMillis();

        trackingMap.forEach((path, state) -> {
            try {
                if (!Files.exists(path)) {
                    trackingMap.remove(path); // Файл удалили
                    return;
                }

                long currentSize = Files.size(path);
                long currentModified = Files.getLastModifiedTime(path).toMillis();

                // Проверяем, изменился ли файл с прошлого раза
                if (currentSize != state.lastSize || currentModified != state.lastModified) {
                    // Файл все еще пишется
                    state.update(currentSize, currentModified, now);
                } else {
                    // Размер стабилен. Прошло достаточно времени тишины?
                    if ((now - state.lastChangeTime) >= config.fileStabilityThreshold()) {

                        // Дополнительная проверка: не залочен ли файл системой
                        if (isOsAccessible(path)) {
                            // !!! Файл готов. Передаем в процессинг !!!
                            trackingMap.remove(path);

                            // Запускаем обработку асинхронно в пуле FileService (или передаем в Executor)
                            CompletableFuture.runAsync(() -> fileService.processFile(path), processingExecutor);
                        }
                    }
                }

                // Проверка на таймаут (если файл висит слишком долго)
                if ((now - state.firstSeenTime) > config.maxFileWaitTime()) {
                    log.warning(">>> Timeout waiting for file stability: " + path);
                    trackingMap.remove(path);
                    // Тут можно переместить файл в папку .error
                }

            } catch (Exception e) {
                log.log(Level.WARNING, "Error checking file stability: " + path, e);
            }
        });
    }

    private boolean isOsAccessible(Path file) {
        // Попытка открыть на чтение для проверки блокировки
        try (var raf = new RandomAccessFile(file.toFile(), "r")) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    // Внутренний класс для хранения состояния
    private static class FileState {
        long lastSize = -1;
        long lastModified = -1;
        long lastChangeTime;
        final long firstSeenTime;

        FileState(long now) {
            this.lastChangeTime = now;
            this.firstSeenTime = now;
        }

        void update(long size, long modified, long now) {
            this.lastSize = size;
            this.lastModified = modified;
            this.lastChangeTime = now;
        }
    }
}