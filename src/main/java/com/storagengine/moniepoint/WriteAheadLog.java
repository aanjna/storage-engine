package com.storagengine.moniepoint;

import java.io.IOException;

public interface WriteAheadLog {
    void append(String record) throws IOException;

    void replay(RecordHandler handler) throws Exception;

    void close() throws Exception;

    interface RecordHandler {
        void handle(String key, String value, String op) throws Exception;
    }

}
