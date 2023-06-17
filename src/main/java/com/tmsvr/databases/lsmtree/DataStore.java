package com.tmsvr.databases.lsmtree;

import com.tmsvr.databases.lsmtree.commitlog.CommitLog;
import com.tmsvr.databases.lsmtree.commitlog.DefaultCommitLog;
import com.tmsvr.databases.lsmtree.memtable.Memtable;
import com.tmsvr.databases.lsmtree.sstable.SSTableManager;

import java.io.IOException;
import java.util.Optional;

public class DataStore {
    private static final long FLUSH_TO_DISK_LIMIT = 5;
    static final String TOMBSTONE = "<TOMBSTONE>";
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

    DataStore(CommitLog commitLog, Memtable memtable, SSTableManager ssTableManager) {
        this.commitLog = commitLog;
        this.memtable = memtable;
        this.ssTableManager = ssTableManager;
    }

    public void put(String key, String value) throws IOException {
        DataRecord dataRecord = new DataRecord(key, value);
        commitLog.append(dataRecord);
        memtable.put(dataRecord);

        if (memtable.getSize() > FLUSH_TO_DISK_LIMIT) {
            flush();
        }
    }

    public Optional<String> get(String key) throws IOException {
        String value = memtable.get(key);

        if (value != null) {
            return Optional.ofNullable(checkDeleted(value));
        } else {
            return ssTableManager.findValue(key).map(this::checkDeleted);
        }
    }

    public void delete(String key) throws IOException {
        put(key, TOMBSTONE);
    }

    public void flush() throws IOException {
        ssTableManager.flush(memtable.getAsMap());
        memtable.clear();
        commitLog.clear();
    }

    private String checkDeleted(String value) {
        return TOMBSTONE.equals(value) ? null : value;
    }
}
