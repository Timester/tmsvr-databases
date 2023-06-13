package com.tmsvr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public final class TestUtils {

    public static void cleanupFiles() throws IOException {
        try (Stream<Path> files = Files.list(Path.of("."))) {
            files
                    .filter(path -> path.getFileName().toString().startsWith("table") || path.getFileName().toString().startsWith("sstable"))
                    .forEach(path1 -> {
                        try {
                            Files.delete(path1);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
        }

        Files.deleteIfExists(Path.of("commit-log.txt"));
    }
}
