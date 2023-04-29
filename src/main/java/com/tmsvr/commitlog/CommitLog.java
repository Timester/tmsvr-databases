package com.tmsvr.commitlog;

import java.io.IOException;
import java.util.List;

public interface CommitLog {
    void append(CommitLogEntry entry) throws IOException;

    List<CommitLogEntry> readCommitLog() throws IOException;

    long getSize();

    void clear() throws IOException;
}
