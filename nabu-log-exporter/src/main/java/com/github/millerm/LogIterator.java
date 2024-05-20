package com.github.millerm;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class LogIterator implements Iterable<ByteBuffer>, AutoCloseable {
    private static LogIterator instance;
    private final String filePath;
    private final int chunkSize;
    private FileChannel fileChannel;
    private long fileSize;
    private long position;
    private ChunkIterator chunkIterator;

    private LogIterator(String filePath, int chunkSize, int position) {
        this.filePath = filePath;
        this.chunkSize = chunkSize;
        this.position = position;

        initialize();

        this.chunkIterator = new ChunkIterator();
    }

    public static synchronized LogIterator getInstance(String filePath, int chunkSize, int position) {
        if (instance == null) {
            instance = new LogIterator(filePath, chunkSize, position);
        }
        return instance;
    }

    private void initialize() {
        try {
            FileInputStream fileInputStream = new FileInputStream(filePath);
            fileChannel = fileInputStream.getChannel();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return chunkIterator;
    }

    private class ChunkIterator implements Iterator<ByteBuffer> {
        @Override
        public boolean hasNext() {
            try {
                // TODO(@millerm) - switch to WatchService
                // (https://docs.oracle.com/javase/8/docs/api/java/nio/file/WatchService.html)
                fileSize = fileChannel.size();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return position < fileSize;
        }

        @Override
        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }

            try {
                ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
                int bytesRead = fileChannel.read(buffer, position);
                if (bytesRead == -1) {
                    throw new NoSuchElementException();
                }
                position += bytesRead;
                buffer.flip();
                return buffer;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setPosition(long newPosition) {
        if (newPosition >= 0 && newPosition <= fileSize) {
            position = newPosition;
        } else {
            throw new IllegalArgumentException("Invalid position: " + newPosition);
        }
    }
}