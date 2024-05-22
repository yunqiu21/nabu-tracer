package com.github.millerm;

public interface LogExporter {
    void run() throws Exception;

    void processLogs();

    void exportLogs(String log);
}
