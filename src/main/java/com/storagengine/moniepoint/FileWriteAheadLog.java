package com.storagengine.moniepoint;

import java.io.*;
import java.nio.file.*;

public class FileWriteAheadLog implements WriteAheadLog{

    private final BufferedWriter writer;
    private final Path walFile;

    public FileWriteAheadLog(String filePath) throws IOException {
        this.walFile = Paths.get(filePath);
        this.writer = Files.newBufferedWriter(walFile, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    @Override
    public synchronized void append(String record) throws IOException {
        writer.write(record);
        writer.newLine();
        writer.flush();
    }

    @Override
    public void replay(RecordHandler handler) throws Exception {
        if (!Files.exists(walFile)) return;

        try (BufferedReader reader = Files.newBufferedReader(walFile)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] p = line.split(",", 3);
                handler.handle(p[1], p.length > 2 ? p[2] : null, p[0]);
            }
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

}
