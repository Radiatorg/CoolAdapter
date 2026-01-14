package com.foranx.cooladapter.watcher;

import com.foranx.cooladapter.config.AppConfig;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class DirectoryWatcher {

    private static final Logger log = Logger.getLogger(DirectoryWatcher.class.getName());

    private final Path rootPath;
    private final FileStabilityMonitor monitor;

    private WatchService watchService;
    private ExecutorService executor;
    private final Map<WatchKey, Path> keys = new ConcurrentHashMap<>();
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DirectoryWatcher(AppConfig config, FileStabilityMonitor monitor) {
        this.rootPath = config.directory();
        this.monitor = monitor;
    }

    public void start() throws IOException {
        if (!running.compareAndSet(false, true)) {
            log.warning("DirectoryWatcher is already running.");
            return;
        }

        log.info(">>> DirectoryWatcher starting at: " + rootPath);

        this.watchService = FileSystems.getDefault().newWatchService();

        this.executor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "DirWatcher-" + r.hashCode());
            t.setDaemon(true);
            return t;
        });

        registerRecursive(rootPath);

        executor.submit(this::scanExistingFiles);
        executor.submit(this::processEvents);
    }

    public void stop() {
        if (!running.compareAndSet(true, false)) return;

        log.info(">>> DirectoryWatcher stopping...");
        try {
            if (watchService != null) watchService.close();
        } catch (IOException e) {
            log.log(Level.WARNING, "Error closing WatchService", e);
        }

        if (executor != null) {
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    log.warning("Executor did not terminate in time");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void registerRecursive(Path start) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<>() {
            @Override
            public @NotNull FileVisitResult preVisitDirectory(@NotNull Path dir, @NotNull BasicFileAttributes attrs) throws IOException {
                if (shouldIgnore(dir)) {
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

    private void scanExistingFiles() {
        try {
            Files.walkFileTree(rootPath, new SimpleFileVisitor<>() {
                @Override
                public @NotNull FileVisitResult preVisitDirectory(@NotNull Path dir, @NotNull BasicFileAttributes attrs) {
                    if (shouldIgnore(dir)) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NotNull FileVisitResult visitFile(@NotNull Path file, @NotNull BasicFileAttributes attrs) {
                    if (Files.isRegularFile(file) && !shouldIgnore(file)) {
                        triggerCallback(file);
                    }
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public @NotNull FileVisitResult visitFileFailed(@NotNull Path file, @NotNull IOException exc) {
                    log.warning("Failed to access file during initial scan: " + file + " (" + exc.getMessage() + ")");
                    return FileVisitResult.CONTINUE; // Не прерываем сканирование
                }
            });
        } catch (IOException e) {
            log.log(Level.SEVERE, "Critical error during initial file scan", e);
        }
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
                log.warning("WatchKey not recognized!");
                key.cancel();
                continue;
            }

            for (WatchEvent<?> event : key.pollEvents()) {
                WatchEvent.Kind<?> kind = event.kind();
                if (kind == StandardWatchEventKinds.OVERFLOW) continue;

                Path name = (Path) event.context();
                Path fullPath = dir.resolve(name);

                if (shouldIgnore(fullPath)) continue;

                try {
                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        if (Files.isDirectory(fullPath)) {
                            registerRecursive(fullPath);
                            scanNewDirectory(fullPath);
                        } else {
                            triggerCallback(fullPath);
                        }
                    } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                        if (Files.isRegularFile(fullPath)) {
                            triggerCallback(fullPath);
                        }
                    }
                } catch (Exception e) {
                    log.log(Level.SEVERE, "Error processing event for: " + fullPath, e);
                }
            }

            if (!key.reset()) {
                keys.remove(key);
            }
        }
        log.info("Event processing loop finished.");
    }

    private void scanNewDirectory(Path start) {
        try (Stream<Path> stream = Files.walk(start)) {
            stream.filter(Files::isRegularFile)
                    .filter(p -> !shouldIgnore(p))
                    .forEach(this::triggerCallback);
        } catch (IOException e) {
            log.warning("Failed to scan new directory: " + start);
        }
    }

    private void triggerCallback(Path file) {
        monitor.watch(file);
    }

    private boolean shouldIgnore(Path path) {
        String name = path.getFileName().toString();
        return name.equals(".processed")
                || name.equals(".error")
                || name.startsWith(".");
    }
}