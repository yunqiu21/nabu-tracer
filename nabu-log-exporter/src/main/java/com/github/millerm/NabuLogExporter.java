package com.github.millerm;

import java.util.logging.Logger;
import java.util.logging.Level;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Path;

public class NabuLogExporter implements LogExporter {
    private static final Logger LOG = Logger.getGlobal();

    private static final int SLEEP_TIME_MILLISECONDS = 5000;

    private static final int MAX_BUFFER_SIZE_BYTES = 100;

    private Path logFilePath;

    private Path stateFilePath;

    private int logPosition;

    public String getLogFilePath() {
        return logFilePath.toString();
    }

    public String getStateFilePath() {
        return stateFilePath.toString();
    }

    public int getLogPosition() {
        return logPosition;
    }

    public void updateLogPosition(int updatedLogPosition) {
        LOG.info("Updated log position: " + updatedLogPosition);
        this.logPosition = updatedLogPosition;
    }

    @Override
    public void run() throws Exception {
        LOG.info("Starting NabuLogExporter...");

        while (true) {
            readLogs();
            exportLogs();
            writeState();
            Thread.sleep(SLEEP_TIME_MILLISECONDS);
        }
    }

    @Override
    public void readLogs() {
        LOG.info("Reading logs from " + getLogFilePath() + "...");
        LOG.info("Log position: " + getLogPosition());

        File logFile = new File(getLogFilePath());

        if (!logFile.exists()) {
            LOG.info("No log file exists...");
            return;
        }

        try (FileChannel channel = new FileInputStream(getLogFilePath()).getChannel()) {
            try (FileLock lock = channel.lock(getLogPosition(), MAX_BUFFER_SIZE_BYTES, true)) {
                java.nio.ByteBuffer buff = java.nio.ByteBuffer.allocate(MAX_BUFFER_SIZE_BYTES);
                StringBuilder logContent = new StringBuilder();

                channel.position(getLogPosition());

                while (channel.read(buff) > 0) {
                    // See:
                    // https://docs.oracle.com/javase%2F9%2Fdocs%2Fapi%2F%2F/java/nio/ByteBuffer.html#flip--
                    buff.flip();

                    while (buff.hasRemaining()) {
                        LOG.info("buffer position: " + buff.position());
                        LOG.info("buffer limit: " + buff.limit());
                        logContent.append((char) buff.get());
                    }

                    updateLogPosition(buff.position());
                    buff.clear();
                    LOG.info("Log content: " + logContent.toString());
                    logContent.setLength(0);
                }

            } catch (Exception e) {
                System.err.println("Error acquiring lock: " + e.getMessage());
                e.printStackTrace();
                return;
            }
        } catch (Exception e) {
            System.err.println("Error opening file: " + e.getMessage());
            e.printStackTrace();
            return;
        }
    }

    @Override
    public void exportLogs() {
        LOG.info("Exporting logs...");
    }

    public void writeState() throws IOException {
        LOG.info("Writing state...");

        try (RandomAccessFile stateFile = new RandomAccessFile(this.stateFilePath.toString(), "rw");
                FileChannel stateFileChannel = stateFile.getChannel()) {

            MappedByteBuffer buffer = stateFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, Integer.MAX_VALUE);

            buffer.putInt(getLogPosition());

            System.out.println("Wrote state: " + getLogPosition());
        }
    }

    @Override
    public void readState() throws IOException {
        LOG.info("Reading state...");

        File stateFile = new File(this.stateFilePath.toString());

        if (!stateFile.exists()) {
            LOG.info("No state exists. Creating...");
            stateFile.createNewFile();
            writeState();

            return;
        }

        try (
                FileInputStream fileInputStream = new FileInputStream(this.stateFilePath.toString());
                BufferedInputStream bufferedInputStream = new BufferedInputStream(fileInputStream);
                DataInputStream dataInputStream = new DataInputStream(bufferedInputStream)) {

            updateLogPosition(dataInputStream.readInt());

            LOG.info("logPosition is: " + getLogPosition());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public NabuLogExporter(String logPath) {
        this.logPosition = 0;
        this.logFilePath = Path.of(logPath);

        try {
            this.readState();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public NabuLogExporter() {
        this.logPosition = 0;
        this.logFilePath = Path.of(System.getenv("NABU_TRACING_LOG_PATH"));
        this.stateFilePath = Path.of(System.getenv("LOG_EXPORTER_STATE_PATH"));

        try {
            this.readState();
        } catch (IOException e) {
            e.printStackTrace();
        }
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
