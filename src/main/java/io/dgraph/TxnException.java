package io.dgraph;

public abstract class TxnException extends RuntimeException {
    TxnException(String message) {
        super(message);
    }
}
