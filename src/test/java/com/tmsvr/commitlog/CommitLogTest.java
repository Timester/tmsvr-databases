package com.tmsvr.commitlog;

import com.tmsvr.DataRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommitLogTest {

    @AfterEach
    @BeforeEach
    void cleanup() throws IOException {
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

        cm.append(new DataRecord("a", "b"));
        cm.append(new DataRecord("a", "c"));
        cm.append(new DataRecord("b", "d"));

        assertEquals(3, cm.getSize());
        List<DataRecord> commitLogEntries = cm.readCommitLog();
        assertEquals(3, commitLogEntries.size());

        assertTrue(commitLogEntries.contains(new DataRecord("a", "b")));
        assertTrue(commitLogEntries.contains(new DataRecord("a", "c")));
        assertTrue(commitLogEntries.contains(new DataRecord("b", "d")));
    }

    @Test
    void clearIsOk() throws IOException {
        CommitLog cm = new DefaultCommitLog();

        cm.append(new DataRecord("a", "b"));
        cm.append(new DataRecord("a", "c"));
        cm.append(new DataRecord("b", "d"));

        assertEquals(3, cm.getSize());
        List<DataRecord> commitLogEntries = cm.readCommitLog();
        assertEquals(3, commitLogEntries.size());

        cm.clear();

        assertEquals(0, cm.getSize());
        assertTrue(cm.readCommitLog().isEmpty());
        assertTrue(Files.exists(Path.of(DefaultCommitLog.FILE_PATH)));
    }
}