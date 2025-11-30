package com.storagengine.moniepoint;

public class KVException extends Exception {
    private final KVMessage kvm;

    public KVException(KVMessage kvm) {
        this.kvm = kvm;
    }

    public KVException(String errorMessage) {
        this.kvm = new KVMessage(KVConstants.RESP, errorMessage);
    }

}
