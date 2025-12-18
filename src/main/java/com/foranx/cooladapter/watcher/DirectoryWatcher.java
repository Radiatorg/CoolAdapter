package com.foranx.cooladapter.watcher;

import com.foranx.cooladapter.config.AppConfig;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

public class DirectoryWatcher {

    private static final Logger log = Logger.getLogger(DirectoryWatcher.class.getName());

    private final Path rootPath;
    private final Consumer<Path> onFileEvent;

    private WatchService watchService;
    private ExecutorService executor;
    private final Map<WatchKey, Path> keys = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    private final Map<Path, Long> eventDebounce = new ConcurrentHashMap<>();

    public DirectoryWatcher(AppConfig config, Consumer<Path> onFileEvent) {
        this.rootPath = Paths.get(config.getDirectory()).toAbsolutePath().normalize();
        this.onFileEvent = onFileEvent;
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) return;

        log.info(">>> DirectoryWatcher starting at: " + rootPath);

        this.watchService = FileSystems.getDefault().newWatchService();
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        registerRecursive(rootPath);
        scanExistingFiles(rootPath);
        executor.submit(this::processEvents);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        log.info(">>> DirectoryWatcher stopping...");
        try {
            if (watchService != null) watchService.close();
        } catch (IOException ignored) {}

        if (executor != null) executor.shutdownNow();
    }

    private void registerRecursive(Path start) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                if (dir.getFileName().toString().equals(".processed")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                registerDirectory(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private void registerDirectory(Path dir) throws IOException {
        WatchKey key = dir.register(watchService,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY);
        keys.put(key, dir);
    }

    private void scanExistingFiles(Path dir) {
        executor.submit(() -> {
            try {
                Files.walk(dir)
                        .filter(Files::isRegularFile)
                        .forEach(this::triggerCallback);
            } catch (IOException e) {
                log.log(Level.WARNING, "Failed to scan existing files", e);
            }
        });
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

                Path name = (Path) event.context();
                Path fullPath = dir.resolve(name);

                if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                    if (Files.isDirectory(fullPath)) {
                        try {
                            registerRecursive(fullPath);
                            scanExistingFiles(fullPath);
                        } catch (IOException e) {
                            log.warning("Failed to register new directory: " + fullPath);
                        }
                    } else {
                        triggerCallback(fullPath);
                    }
                } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                    if (Files.isRegularFile(fullPath)) {
                        triggerCallback(fullPath);
                    }
                }
            }

            if (!key.reset()) {
                keys.remove(key);
            }
        }
    }

    private void triggerCallback(Path file) {
        long now = System.currentTimeMillis();
        Long lastTime = eventDebounce.get(file);
        if (lastTime != null && (now - lastTime) < 2000) {
            return;
        }
        eventDebounce.put(file, now);

        executor.submit(() -> {
            try {
                onFileEvent.accept(file);
                Thread.sleep(2500);
                eventDebounce.remove(file);
            } catch (Exception e) {
                log.log(Level.WARNING, "Error in file callback", e);
            }
        });
    }
}