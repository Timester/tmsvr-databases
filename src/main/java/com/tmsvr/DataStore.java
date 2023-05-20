package com.tmsvr;

import com.tmsvr.commitlog.CommitLog;
import com.tmsvr.commitlog.DefaultCommitLog;
import com.tmsvr.memtable.Memtable;
import com.tmsvr.sstable.SSTableManager;

import java.io.IOException;
import java.util.Optional;

public class DataStore {
    private final CommitLog commitLog;
    private final Memtable memtable;
    private final SSTableManager ssTableManager;

    public DataStore() throws IOException {
        this.commitLog = new DefaultCommitLog();

        if (this.commitLog.getSize() > 0) {
            this.memtable = new Memtable(this.commitLog.readCommitLog());
        } else {
            this.memtable = new Memtable();
        }

        ssTableManager = new SSTableManager();
        ssTableManager.readTablesFromFile();
    }

    public void store(String key, String value) throws IOException {
        DataRecord dataRecord = new DataRecord(key, value);
        commitLog.append(dataRecord);
        memtable.put(dataRecord);
    }

    public Optional<String> get(String key) throws IOException {
        String value = memtable.get(key);

        if (value != null) {
            return Optional.of(value);
        } else {
            return ssTableManager.findValue(key);
        }
    }

    public void flush() throws IOException {
        ssTableManager.flush(memtable.getAsMap());
        memtable.clear();
        commitLog.clear();
    }

}
