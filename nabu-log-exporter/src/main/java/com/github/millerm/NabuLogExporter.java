package com.github.millerm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URI;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class NabuLogExporter implements LogExporter {
    private static final Logger LOG = Logger.getGlobal();

    private static final int SLEEP_TIME_MILLISECONDS = 1;

    private static final int MAX_LOG_FILE_SIZE = 200 * 1024 * 1024;

    private final Path LOG_PATH_DIR;

    private Path stateFilePath;

    private final ExecutorService executorService;

    public String getLogFilePath() {
        return LOG_PATH_DIR.toString();
    }

    public String getStateFilePath() {
        return stateFilePath.toString();
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

    @Override
    public void processLogs() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(LOG_PATH_DIR)) {
            LOG.info("Checking for existing files in log path: " + LOG_PATH_DIR);

            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    if (Files.size(entry) < MAX_LOG_FILE_SIZE) {
                        executorService.submit(new LogTask(entry, stateFilePath));
                    }

                }
            }
        } catch (IOException e) {
            LOG.severe("Error reading existing files: " + e.getMessage());
        }

        try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
            LOG_PATH_DIR.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);

            LOG.info("Watching for new log files at: " + LOG_PATH_DIR);

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
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    Path filePath = LOG_PATH_DIR.resolve(filename);

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        LOG.info("New file detected: " + filePath);
                        executorService.submit(new LogTask(filePath, stateFilePath));
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

    private static class LogTask implements Runnable {
        private final Path filePath;
        private final String stateFilePath;
        long filePointer;

        public LogTask(Path filePath, Path stateFilePath) {
            this.filePath = filePath;
            this.stateFilePath = stateFilePath.toString() + "/" + filePath.getFileName().toString();
            this.filePointer = readState();
        }

        private static String parseLog(String line) {
            String[] parts = line.split(" ", 7);

            // TODO: better parse
            return "{" +
                    "\"traceId\":\"" + parts[0] + "\"," +
                    "\"spanId\":\"" + parts[1] + "\"," +
                    "\"operationName\":\"" + parts[5] + "\"," +
                    "\"references\":[]," +
                    "\"startTimeUnixNano\":\"" + "1716525641" + "\"," +
                    "\"endTimeUnixNano\":\"" + "1716525642" + "\"," +
                    "\"kind\":2" +
                    "}";

        }

        private static void handleLog(String line) {
            HttpClient client = HttpClient.newHttpClient();
            URI uri = URI.create("http://localhost:4318/v1/traces");

            StringBuilder spansArray = new StringBuilder();
            spansArray.append("[");
            String span = parseLog(line);
            spansArray.append(span);
            spansArray.append("]");

            // Wrap the spans array in a root JSON object
            String requestBody = "{\"resourceSpans\":[" +
                    "{\"resource\":{\"attributes\":[{\"key\":\"service.name\",\"value\":{\"stringValue\":\"nabu-tracer-tester2\"}}]},"
                    +
                    "\"scopeSpans\":[{\"scope\":{\"name\":\"manual-test\"},\"spans\":"
                    + spansArray.toString() + "}]}]}";

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
                        Thread.sleep(SLEEP_TIME_MILLISECONDS);
                        reader.seek(reader.getFilePointer()); // Reset the file pointer to the current position
                    }
                }
            } catch (IOException | InterruptedException e) {
                LOG.severe("Error reading log file: " + e.getMessage());
                Thread.currentThread().interrupt();
            }
        }

        private void saveState(long position) {
            try (BufferedWriter writer = Files.newBufferedWriter(Path.of(stateFilePath), StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                writer.write(Long.toString(position));
                writer.flush();

                LOG.info("Saved state at position: " + position);
            } catch (IOException e) {
                LOG.severe("Error saving state: " + e.getMessage());
            }
        }

        private long readState() {
            if (Files.exists(Path.of(stateFilePath))) {
                try (BufferedReader reader = Files.newBufferedReader(Path.of(stateFilePath))) {
                    String line = reader.readLine();

                    if (line != null) {
                        return Long.parseLong(line);
                    }
                } catch (IOException e) {
                    LOG.severe("Error reading state: " + e.getMessage());
                    return 0;
                }
            }
            return 0;
        }
    }

    public NabuLogExporter(String logPath) {
        this.LOG_PATH_DIR = Path.of(logPath);
        this.executorService = Executors.newCachedThreadPool();
    }

    public NabuLogExporter() {
        this.LOG_PATH_DIR = Path.of(System.getenv("NABU_TRACING_LOG_PATH"));
        this.stateFilePath = Path.of(System.getenv("LOG_EXPORTER_STATE_PATH"));
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
