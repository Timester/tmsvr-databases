package com.tmsvr.sstable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SSTableManager {
    private final List<SSTable> ssTables;

    public SSTableManager() {
        this.ssTables = new ArrayList<>();
    }

    public void flush(Map<String, String> data) throws IOException {
        SSTable ssTable = new SSTable("sstable-" + ssTables.size());
        ssTable.write(data);
        ssTables.add(ssTable);
    }

    public String findValue(String key) throws IOException {
        for (int i = ssTables.size() -1 ; i >= 0; i--) {
            String value = ssTables.get(i).getValue(key);
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    public void readTablesFromFile() throws IOException {
        Path rootPath = Path.of("");
        Files.find(rootPath, 1, (path, attr) -> path.toString().endsWith(".index"))
                .forEach(path -> {
                    System.out.println("SSTable found: " + path.toString().replace(".index", ""));
                    try {
                        ssTables.add(new SSTable(path.toString().replace(".index", "")));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    public void compaction() {
        // TODO: implement compaction
    }
}
