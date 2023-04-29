package com.tmsvr.commitlog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;

public class DefaultCommitLog implements CommitLog {

    static final String FILE_PATH = "commitlog.txt";
    private long size;

    public DefaultCommitLog() throws IOException {
        this.size = 0;

        if (Files.exists(Paths.get(FILE_PATH))) {
            System.out.println("Commit Log already exists");
            size = countLinesInLog();
        } else {
            createFile();
        }
    }

    private void createFile() throws IOException {
        Files.createFile(Paths.get(FILE_PATH));
    }

    private long countLinesInLog() throws IOException {
        return Files.lines(Paths.get(DefaultCommitLog.FILE_PATH)).count();
    }

    private String entryToString(CommitLogEntry entry) {
        return entry.key() + "::" + entry.value() + System.lineSeparator();
    }

    @Override
    public void append(CommitLogEntry entry) throws IOException {
        Files.write(Paths.get(FILE_PATH), entryToString(entry).getBytes(), StandardOpenOption.APPEND);
        size++;
    }

    @Override
    public List<CommitLogEntry> readCommitLog() throws IOException {
        return Files.readAllLines(Paths.get(FILE_PATH))
                .stream()
                .map(line -> {
                    String[] split = line.split("::");
                    return new CommitLogEntry(split[0], split[1]);
                }).toList();
    }

    @Override
    public long getSize() {
        return size;
    }

    @Override
    public void clear() throws IOException {
        Files.deleteIfExists(Paths.get(FILE_PATH));
        createFile();
        size = 0;
    }
}
