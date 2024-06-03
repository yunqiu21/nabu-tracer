package com.github.millerm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributes;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NabuLogExporter implements LogExporter {
    private static final Logger LOG = Logger.getGlobal();

    private static final int LOG_EXPORT_POLL_DELAY_MILLISECONDS = 1;

    private static final int MAX_LOG_FILE_SIZE = 200 * 1024 * 1024;

    private static final String LOG_FILE_IDENTIFIER = "trace.log";

    private final Path logPathDir;

    private Path stateDirPath;

    private final ExecutorService executorService;

    public String getLogFilePath() {
        return logPathDir.toString();
    }

    public String getstateDirPath() {
        return stateDirPath.toString();
    }

    @Override
    public void run() throws Exception {
        LOG.info("Starting NabuLogExporter...");

        while (true) {
            processLogs();
        }
    }

    @Override
    public void exportLogs(String log) {
        LOG.info("Exporting log: " + log);
    }

    public void handleExistingLogFiles(Path logPathDir) {
        LOG.info("Checking for existing log files in path: " + logPathDir.toString());

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logPathDir)) {
            LOG.info("Checking for existing files in log path: " + logPathDir);

            for (Path entry : stream) {
                if (Files.isDirectory(entry)) {
                    handleExistingLogFiles(entry);
                } else if (Files.isRegularFile(entry)) {
                    if (Files.size(entry) < MAX_LOG_FILE_SIZE) {
                        if (entry.toString().endsWith(LOG_FILE_IDENTIFIER)) {
                            executorService.submit(new LogTask(entry, stateDirPath));
                        }
                    }

                }
            }
        } catch (IOException e) {
            LOG.severe("Error reading existing files: " + e.getMessage());
        }
    }

    private void registerAll(Path start, WatchService watchService) throws IOException {
        Files.walkFileTree(start, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    public void handleNewLogFiles(Path logDirPath) {
        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            logPathDir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            LOG.info("Watching for new log files at: " + logPathDir);

            while (true) {
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.severe("Directory watching interrupted");
                    return;
                }

                for (WatchEvent<?> event : key.pollEvents()) {
                    WatchEvent.Kind<?> kind = event.kind();

                    if (kind == StandardWatchEventKinds.OVERFLOW) {
                        continue;
                    }

                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> watch_event = (WatchEvent<Path>) event;
                    Path filename = watch_event.context();
                    LOG.info("TEST: + " + filename.toString());
                    Path filePath = logPathDir.resolve(filename);

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        // Handle all directories (including nested)
                        if (Files.isDirectory(filePath)) {
                            try {
                                registerAll(filePath, watchService);
                            } catch (IOException e) {
                                LOG.severe("Error registering new directory: " + e.getMessage());
                            }
                        } else if (filePath.toString().endsWith(LOG_FILE_IDENTIFIER)) {
                            LOG.info("New file detected: " + filePath);
                            executorService.submit(new LogTask(filePath, stateDirPath));
                        }
                    } else if (kind == StandardWatchEventKinds.ENTRY_MODIFY) {
                        if (Files.isRegularFile(filePath) && filePath.toString().endsWith(LOG_FILE_IDENTIFIER)) {
                            LOG.info("File modified: " + filePath);
                            executorService.submit(new LogTask(filePath, stateDirPath));
                        }
                    }
                }

                boolean valid = key.reset();
                if (!valid) {
                    break;
                }
            }
        } catch (IOException e) {
            LOG.severe("Error watching directory: " + e.getMessage());
        }
    }

    @Override
    public void processLogs() {
        handleExistingLogFiles(logPathDir);
        handleNewLogFiles(logPathDir);
    }

    private static class LogTask implements Runnable {
        private final Path filePath;
        private final String stateDirPath;
        private State state;
        long filePointer;

        public LogTask(Path filePath, Path stateDirPath) {
            this.filePath = filePath;

            this.stateDirPath = stateDirPath.toString() + "/" + filePath.getFileName().toString();
            try {
                state = new State(this.stateDirPath);
                this.filePointer = readState();
            } catch (IOException e) {
                LOG.severe("Error initiating state: " + e);
                Thread.currentThread().interrupt();
            }
        }

        private static String parseLog(String line) {
            String[] parts = line.split("\t");

            if (parts.length == 7) {
                return "{" +
                        "\"traceId\":\"" + parts[0] + "\"," +
                        "\"nodeId\":\"" + parts[1] + "\"," +
                        "\"peerNodeId\":\"" + "{}" + "\"," +
                        "\"threadId\":\"" + parts[2] + "\"," +
                        "\"timestamp\":\"" + parts[3] + "\"," +
                        "\"eventType\":\"" + parts[5] + "\"" +
                        "}";
            } else if (parts.length == 8) {
                return "{" +
                        "\"traceId\":\"" + parts[0] + "\"," +
                        "\"nodeId\":\"" + parts[1] + "\"," +
                        "\"peerNodeId\":\"" + parts[2] + "\"," +
                        "\"threadId\":\"" + parts[3] + "\"," +
                        "\"timestamp\":\"" + parts[4] + "\"," +
                        "\"eventType\":\"" + parts[6] + "\"" +
                        "}";
            }

            return "{}";
        }

        private static void handleLog(String line) {
            HttpClient client = HttpClient.newHttpClient();
            URI uri = URI.create("http://34.171.111.18:5200/v3/buildspan");

            String requestBody = parseLog(line);
            LOG.info("Sending " + requestBody);

            HttpRequest request = HttpRequest.newBuilder()
                    .uri(uri)
                    .header("Content-Type", "application/json")
                    .POST(BodyPublishers.ofString(requestBody))
                    .build();

            try {
                HttpResponse<String> response = client.send(request, BodyHandlers.ofString());
                LOG.info("Response status code: " + response.statusCode());
                LOG.info("Response body: " + response.body());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            try (RandomAccessFile reader = new RandomAccessFile(filePath.toFile(), "r")) {

                String logEntry;

                // Initially, set the reader to position read from state
                reader.seek(filePointer);
                while (true) {
                    // NOTE(@millerm): Can switch to read() to read a specific amount
                    // of bytes if necessary
                    logEntry = reader.readLine();

                    if (logEntry != null) {
                        LOG.info("New log: " + logEntry);

                        handleLog(logEntry);
                        saveState(reader.getFilePointer());
                    } else {
                        Thread.sleep(LOG_EXPORT_POLL_DELAY_MILLISECONDS);
                        reader.seek(reader.getFilePointer()); // Reset the file pointer to the current position
                    }
                }
            } catch (IOException | InterruptedException e) {
                LOG.severe("Error reading log file: " + e.getMessage());
                state.close();
                Thread.currentThread().interrupt();
            }
        }

        private void saveState(long position) {
            state.saveState(position);
        }

        private long readState() {
            return state.readState();
        }

    }

    public NabuLogExporter(String logPath) {
        this.logPathDir = Path.of(logPath);
        this.executorService = Executors.newCachedThreadPool();
    }

    public NabuLogExporter() {
        this.logPathDir = Path.of(System.getenv("NABU_TRACING_LOG_PATH"));
        this.stateDirPath = Path.of(System.getenv("LOG_EXPORTER_STATE_PATH"));
        this.executorService = Executors.newCachedThreadPool();
    }

    public static void main(String[] args) {
        try {
            NabuLogExporter nabuLogExporter = new NabuLogExporter();
            nabuLogExporter.run();
        } catch (Exception e) {
            LOG.log(Level.SEVERE, "SHUTDOWN", e);
        }
    }
}
