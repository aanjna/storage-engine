package com.storagengine.moniepoint;

import java.io.Serializable;

public class KVMessage implements Serializable {

    private String msgType;
    private String key;
    private String value;
    private String message;

    public KVMessage(String msgType) {
        this(msgType, null);
    }

    public KVMessage(String msgType, String message) {
        this.msgType = msgType;
        this.message = message;
    }

}
