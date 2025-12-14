package com.foranx.cooladapter.core;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class DirectoryWatcher {

    private static final Logger log = Logger.getLogger(DirectoryWatcher.class.getName());
    private final Path rootPath;

    private WatchService watchService;
    private ExecutorService executor;

    private final Map<WatchKey, Path> keys = new ConcurrentHashMap<>();

    private final Map<Path, Long> processedFiles = new ConcurrentHashMap<>();
    private static final long PROCESS_TTL_MS = 2000;

    private final AtomicBoolean running = new AtomicBoolean(false);

    public DirectoryWatcher(String directory) {
        this.rootPath = Paths.get(directory).toAbsolutePath().normalize();
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            return;
        }

        log.info(">>> DirectoryWatcher starting...");
        log.info(">>> Root directory: " + rootPath);

        this.watchService = FileSystems.getDefault().newWatchService();
        this.executor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors()
        );

        registerAll(rootPath);

        executor.submit(this::processEvents);

        log.info(">>> DirectoryWatcher started.");
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        log.info(">>> DirectoryWatcher stopping...");

        try {
            if (watchService != null) {
                watchService.close();
            }
        } catch (IOException ignored) {}

        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.info(">>> Executor did not terminate in time");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        log.info(">>> DirectoryWatcher stopped.");
    }

    private void registerAll(Path start) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                registerDirectory(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void registerDirectory(Path dir) throws IOException {
        WatchKey key = dir.register(
                watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE,
                StandardWatchEventKinds.ENTRY_MODIFY
        );
        keys.put(key, dir);
    }

    private void processEvents() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            WatchKey key;
            try {
                key = watchService.take();
            } catch (InterruptedException | ClosedWatchServiceException e) {
                break;
            }

            Path dir = keys.get(key);
            if (dir == null) {
                key.reset();
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) continue;

                @SuppressWarnings("unchecked")
                Path name = ((WatchEvent<Path>) event).context();
                Path fullPath = dir.resolve(name);

                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    if (Files.isDirectory(fullPath, LinkOption.NOFOLLOW_LINKS)) {
                        log.info(">>> NEW DIR: " + fullPath);
                        try {
                            registerAll(fullPath);
                            scanNewFolder(fullPath);
                        } catch (IOException e) {
                            log.log(Level.WARNING, "Failed to register or scan new directory: " + fullPath, e);
                        }
                    } else {
                        handleFile(fullPath, "CREATE");
                    }
                }

                if (kind == StandardWatchEventKinds.ENTRY_MODIFY && Files.isRegularFile(fullPath)) {
                    handleFile(fullPath, "MODIFY");
                }

                if (kind == StandardWatchEventKinds.ENTRY_DELETE) {
                    log.info(">>> DELETE: " + fullPath);
                }
            }

            boolean valid = key.reset();
            if (!valid) {
                keys.remove(key);
            }
        }
    }

    private void handleFile(Path file, String eventType) {
        long now = System.currentTimeMillis();

        Long last = processedFiles.get(file);
        if (last != null && (now - last) < PROCESS_TTL_MS) {
            return;
        }

        log.info(">>> FILE EVENT [" + eventType + "]: " + file);
        processedFiles.put(file, now);

        executor.submit(() -> {
            try {
                Thread.sleep(PROCESS_TTL_MS);
                processedFiles.remove(file);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        });
    }

    private void scanNewFolder(Path folder) {
        executor.submit(() -> {
            try {
                Thread.sleep(100);
                Files.walkFileTree(folder, new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        handleFile(file, "CREATE_SCAN");
                        return FileVisitResult.CONTINUE;
                    }
                });
            } catch (Exception e) {
                log.log(Level.SEVERE, "Failed to scan folder: " + folder, e);
            }
        });
    }
}
