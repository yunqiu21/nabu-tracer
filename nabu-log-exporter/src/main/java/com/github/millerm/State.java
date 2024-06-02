package com.github.millerm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.logging.Logger;
import java.nio.file.Files;
import java.nio.file.Paths;

public class State {
    private static final Logger LOG = Logger.getLogger(State.class.getName());
    private final FileChannel fileChannel;

    public State(String stateDirPath) throws IOException {
        Path path = Paths.get(stateDirPath);

        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path.getParent());
                Files.createFile(path);
            } catch (IOException e) {
                LOG.severe("Error creating state file: " + e);
            }
        }

        fileChannel = FileChannel.open(
                Path.of(stateDirPath),
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    public void saveState(long position) {
        try {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(position);
            buffer.flip();

            fileChannel.position(0);
            fileChannel.write(buffer);
            fileChannel.force(true);
        } catch (IOException e) {
            LOG.severe("Error saving state: " + e.getMessage());
        }
    }

    public long readState() {
        try {
            if (fileChannel.size() == 0) {
                LOG.info("State file is empty.");
                return 0;
            }

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            fileChannel.position(0);
            int bytesRead = fileChannel.read(buffer);
            if (bytesRead == Long.BYTES) {
                buffer.flip();
                return buffer.getLong();
            }
        } catch (IOException e) {
            LOG.severe("Error reading state: " + e);
        }
        return 0;
    }

    public void close() {
        try {
            fileChannel.close();
        } catch (IOException e) {
            LOG.severe("Error closing file channel: " + e.getMessage());
        }
    }
}
