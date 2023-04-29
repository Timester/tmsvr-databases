package com.tmsvr.commitlog;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommitLogTest {

    @BeforeEach
    void setUp() throws IOException {
        Files.deleteIfExists(Path.of(DefaultCommitLog.FILE_PATH));
    }

    @Test
    void creationIsOk() throws IOException {
        CommitLog cm = new DefaultCommitLog();

        assertEquals(0, cm.getSize());
        assertTrue(cm.readCommitLog().isEmpty());
        assertTrue(Files.exists(Path.of(DefaultCommitLog.FILE_PATH)));
    }

    @Test
    void appendAndReadIsOk() throws IOException {
        CommitLog cm = new DefaultCommitLog();

        cm.append(new CommitLogEntry("a", "b"));
        cm.append(new CommitLogEntry("a", "c"));
        cm.append(new CommitLogEntry("b", "d"));

        assertEquals(3, cm.getSize());
        List<CommitLogEntry> commitLogEntries = cm.readCommitLog();
        assertEquals(3, commitLogEntries.size());

        assertTrue(commitLogEntries.contains(new CommitLogEntry("a", "b")));
        assertTrue(commitLogEntries.contains(new CommitLogEntry("a", "c")));
        assertTrue(commitLogEntries.contains(new CommitLogEntry("b", "d")));
    }

    @Test
    void clearIsOk() throws IOException {
        CommitLog cm = new DefaultCommitLog();

        cm.append(new CommitLogEntry("a", "b"));
        cm.append(new CommitLogEntry("a", "c"));
        cm.append(new CommitLogEntry("b", "d"));

        assertEquals(3, cm.getSize());
        List<CommitLogEntry> commitLogEntries = cm.readCommitLog();
        assertEquals(3, commitLogEntries.size());

        cm.clear();

        assertEquals(0, cm.getSize());
        assertTrue(cm.readCommitLog().isEmpty());
        assertTrue(Files.exists(Path.of(DefaultCommitLog.FILE_PATH)));
    }
}