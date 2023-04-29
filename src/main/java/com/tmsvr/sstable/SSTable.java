package com.tmsvr.sstable;

import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class SSTable {
    private static final String INDEX_FILE_SUFFIX = ".index";
    private static final String DATA_FILE_SUFFIX = ".data";

    private final Path indexFile;
    private final Path dataFile;
    private final Map<String, Long> index;

    public SSTable(String filename) throws IOException {
        this.indexFile = Paths.get(filename + INDEX_FILE_SUFFIX);
        this.dataFile = Paths.get(filename + DATA_FILE_SUFFIX);
        this.index = loadIndex();
    }

    public void write(Map<String, String> data) throws IOException {
        if (Files.exists(indexFile)) {
            System.out.println("SSTable can't be written, Index file already exists");
            return;
        }

        // Write data to a temporary file
        Path tempFile = Files.createFile(Path.of(dataFile.getFileName().toString() + ".tmp"));

        // Write the data to the temporary file and create an index
        Map<String, Long> newIndex = new TreeMap<>();
        long offset = 0;
        for (Map.Entry<String, String> entry : data.entrySet()) {
            Files.write(tempFile, (entry.getKey() + "::" + entry.getValue() + System.lineSeparator()).getBytes(), StandardOpenOption.APPEND);
            newIndex.put(entry.getKey(), offset);
            offset++;
        }

        // Write the index to the index file
        Files.createFile(indexFile);
        try (ObjectOutputStream indexObjOut = new ObjectOutputStream(new FileOutputStream(indexFile.toFile()))) {
            indexObjOut.writeObject(newIndex);
        }

        // Rename the temporary file to the data file
        Files.move(tempFile, dataFile);

        // Update the in-memory index
        index.clear();
        index.putAll(newIndex);
    }

    public String getValue(String key) throws IOException {
        Long offset = index.get(key);
        if (offset == null) {
            return null;
        }

        String result;
        try (Stream<String> lines = Files.lines(dataFile)) {
            result = lines.skip(offset).findFirst().get();
        }

        String foundKey = result.split("::")[0];

        if (!foundKey.equals(key)) {
            throw new IllegalStateException("Unexpected key: " + foundKey);
        }
        return result.split("::")[1];
    }

    private Map<String, Long> loadIndex() throws IOException {
        try (ObjectInputStream indexObjIn = new ObjectInputStream(new FileInputStream(indexFile.toFile()))) {
            return (Map<String, Long>) indexObjIn.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to load index", e);
        } catch (EOFException | FileNotFoundException e) {
            System.out.println("Index file is empty");
            return new TreeMap<>();
        }
    }
}
