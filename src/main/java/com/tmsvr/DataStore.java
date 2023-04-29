package com.tmsvr;

import com.tmsvr.commitlog.CommitLog;
import com.tmsvr.commitlog.DefaultCommitLog;
import com.tmsvr.memtable.Memtable;

import java.io.IOException;

public class DataStore {
    private final CommitLog commitLog;
    private final Memtable memtable;

    public DataStore() throws IOException {
        this.commitLog = new DefaultCommitLog();

        if (this.commitLog.getSize() > 0) {
            this.memtable = new Memtable(this.commitLog.readCommitLog());
        } else {
            this.memtable = new Memtable();
        }
    }

    public void store(String key, String value) throws IOException {
        DataRecord dataRecord = new DataRecord(key, value);
        commitLog.append(dataRecord);
        memtable.put(dataRecord);
    }

    public String get(String key) {
        return memtable.get(key);
    }
}
