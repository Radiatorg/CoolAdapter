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

public class FileStabilityMonitor {

    private static final Logger log = Logger.getLogger(FileStabilityMonitor.class.getName());

    private final AppConfig config;
    private final FileService fileService;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService processingExecutor;

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
                    trackingMap.remove(path);
                    return;
                }

                long currentSize = Files.size(path);
                long currentModified = Files.getLastModifiedTime(path).toMillis();

                if (currentSize != state.lastSize || currentModified != state.lastModified) {
                    state.update(currentSize, currentModified, now);
                } else {
                    if ((now - state.lastChangeTime) >= config.fileStabilityThreshold()) {

                        if (isOsAccessible(path)) {
                            trackingMap.remove(path);

                            CompletableFuture.runAsync(() -> fileService.processFile(path), processingExecutor);
                        }
                    }
                }

                if ((now - state.firstSeenTime) > config.maxFileWaitTime()) {
                    log.warning(">>> Timeout waiting for file stability: " + path);
                    trackingMap.remove(path);
                }

            } catch (Exception e) {
                log.log(Level.WARNING, "Error checking file stability: " + path, e);
            }
        });
    }

    private boolean isOsAccessible(Path file) {
        try (var ignored = new RandomAccessFile(file.toFile(), "r")) {
            return true;
        } catch (IOException e) {
            return false;
        }
    }

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