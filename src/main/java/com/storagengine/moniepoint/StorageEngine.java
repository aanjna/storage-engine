package com.storagengine.moniepoint;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class StorageEngine implements KeyValueStore {
    private final Path dataFile;
    private final RandomAccessFile raf;
    //Using a ConcurrentSkipListMap to maintain an in-memory sorted index enabling efficient key-range queries.
    private final ConcurrentSkipListMap<String, Long> index = new ConcurrentSkipListMap<>();
    private final Object writeLock = new Object();

    public StorageEngine(Path storagePath) throws IOException {
        this.dataFile = storagePath.resolve("data.log");
        this.raf = new RandomAccessFile(dataFile.toFile(), "rw");
        loadIndex();
    }

    private void loadIndex() throws IOException {
        long pos = 0;
        raf.seek(0);
        while (pos < raf.length()) {
            raf.seek(pos);
            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            String key = new String(keyBytes);

            int valLen = raf.readInt();
            raf.skipBytes(valLen); // Skip value bytes

            index.put(key, pos);
            pos = raf.getFilePointer();
        }
    }



    @Override
    public void put(String key, byte[] value) throws IOException {
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        synchronized (writeLock) {
            raf.seek(raf.length());
            long pos = raf.getFilePointer();

            byte[] keyBytes = key.getBytes();
            raf.writeInt(keyBytes.length);
            raf.write(keyBytes);
            raf.writeInt(value.length);
            raf.write(value);
            raf.getFD().sync();

            index.put(key, pos);
        }
    }

    @Override
    public Optional<byte[]> read(String key) throws IOException {
        Long pos = index.get(key);
        if (pos == null) return Optional.empty();

        synchronized (raf) {
            raf.seek(pos);
            int keyLen = raf.readInt();
            raf.skipBytes(keyLen);
            int valLen = raf.readInt();
            byte[] valBytes = new byte[valLen];
            raf.readFully(valBytes);
            return Optional.of(valBytes);
        }
    }

    @Override
    public NavigableMap<String, byte[]> readKeyRange(String startKey, String endKey) throws IOException {
        NavigableMap<String, Long> subIndex = index.subMap(startKey, true, endKey, true);
        NavigableMap<String, byte[]> results = new TreeMap<>();
        for (var entry : subIndex.entrySet()) {
            read(entry.getKey()).ifPresent(value -> results.put(entry.getKey(), value));
        }
        return results;
    }

    @Override
    public void batchPut(Map<String, byte[]> entries) throws IOException {
        synchronized (writeLock) {
            for (var e : entries.entrySet()) {
                put(e.getKey(), e.getValue());
            }
        }
    }

    @Override
    public void delete(String key) throws IOException {
        put(key, new byte[0]);  // tombstone entry for delete
    }
}
