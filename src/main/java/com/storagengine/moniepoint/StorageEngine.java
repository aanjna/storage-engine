package com.storagengine.moniepoint;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class StorageEngine implements KeyValueStore {

    //Using a ConcurrentSkipListMap to maintain an in-memory sorted index enabling efficient key-range queries.
    // high-concurrency sorted map
    private final ConcurrentSkipListMap<String, byte[]> memTable = new ConcurrentSkipListMap<>();
    private final WriteAheadLog wal;

    // threshold before flushing to disk
    private static final int MEMTABLE_LIMIT = 10_000;

    public StorageEngine(WriteAheadLog wal) throws Exception {
        this.wal = wal;
        recover();
    }

    private void recover() throws Exception {
        wal.replay((key, value, op) -> {
            if ("PUT".equals(op)) memTable.put(key, Base64.getDecoder().decode(value));
            else if ("DEL".equals(op)) memTable.remove(key);
        });
    }

    private void walPut(String key, byte[] value) throws IOException {
        wal.append("PUT," + key + "," + Base64.getEncoder().encodeToString(value));
    }

    private void walDelete(String key) throws IOException {
        wal.append("DEL," + key);
    }

    private synchronized void flushToSSTable() throws IOException {
        // TODO: Add actual SSTable writer logic.
        memTable.clear();
    }

    // -------------------------
    // Core KV Operations
    // -------------------------

    @Override
    public void put(String key, byte[] value) throws IOException {
        walPut(key, value);
        memTable.put(key, value);

        if (memTable.size() >= MEMTABLE_LIMIT) {
            flushToSSTable();
        }
    }

    @Override
    public Optional<byte[]> read(String key) {
        return Optional.ofNullable(memTable.get(key));
    }

    @Override
    public NavigableMap<String, byte[]> readKeyRange(String startKey, String endKey) {
        return memTable.subMap(startKey, true, endKey, true);
    }

    @Override
    public void batchPut(Map<String, byte[]> kvs) throws IOException {
        for (var e : kvs.entrySet()) {
            walPut(e.getKey(), e.getValue());
            memTable.put(e.getKey(), e.getValue());
        }

        if (memTable.size() >= MEMTABLE_LIMIT) {
            flushToSSTable();
        }
    }

    @Override
    public void delete(String key) throws IOException {
        walDelete(key);
        memTable.remove(key);
    }


}
