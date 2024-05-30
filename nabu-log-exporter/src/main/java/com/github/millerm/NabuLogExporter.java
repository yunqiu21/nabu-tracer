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

    private static final int LOG_EXPORT_POLL_DELAY_MILLISECONDS = 1;

    private static final int MAX_LOG_FILE_SIZE = 200 * 1024 * 1024;

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

    @Override
    public void processLogs() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(logPathDir)) {
            LOG.info("Checking for existing files in log path: " + logPathDir);

            for (Path entry : stream) {
                if (Files.isRegularFile(entry)) {
                    if (Files.size(entry) < MAX_LOG_FILE_SIZE) {
                        executorService.submit(new LogTask(entry, stateDirPath));
                    }

                }
            }
        } catch (IOException e) {
            LOG.severe("Error reading existing files: " + e.getMessage());
        }

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
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path filename = ev.context();
                    Path filePath = logPathDir.resolve(filename);

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        LOG.info("New file detected: " + filePath);
                        executorService.submit(new LogTask(filePath, stateDirPath));
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
                LOG.severe("Error reading log file: " + e.getMessage());
                state.close();
                Thread.currentThread().interrupt();
            }
        }

        private static String parseLog(String line) {
            String[] parts = line.split(" ", 7);

            // TODO(@millerm): better parse
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

                        // handleLog(logEntry);
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
