package com.storagengine.moniepoint;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;

public interface KeyValueStore {
//    Put(Key, Value)
//2. Read(Key)
//3. ReadKeyRange(StartKey, EndKey)
//4. BatchPut(..keys, ..values)
//5. Delete(key)

    void put(String key, byte[] value) throws IOException;
    Optional<byte[]> read(String key) throws Exception;
    NavigableMap<String, byte[]> readKeyRange(String startKey, String endKey) throws Exception;
    void batchPut(Map<String, byte[]> kvs) throws IOException;
    void delete(String key) throws IOException;

}
