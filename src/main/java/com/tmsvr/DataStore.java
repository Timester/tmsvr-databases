package com.tmsvr;

import com.tmsvr.commitlog.CommitLog;
import com.tmsvr.commitlog.CommitLogEntry;
import com.tmsvr.commitlog.DefaultCommitLog;

import java.io.IOException;

public class DataStore {
    private final CommitLog commitLog;

    public DataStore() throws IOException {
        this.commitLog = new DefaultCommitLog();
    }

    public void store(String key, String value) throws IOException {
        commitLog.append(new CommitLogEntry(key, value));
    }
}
