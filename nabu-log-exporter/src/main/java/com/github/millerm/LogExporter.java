package com.github.millerm;

import java.io.IOException;

public interface LogExporter {
    void run() throws Exception;

    void readLogs();

    void exportLogs();

    void writeState() throws IOException;

    void readState() throws IOException;
}
