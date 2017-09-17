package io.dgraph.exception;

public class DGraphException extends RuntimeException {

    private static final long serialVersionUID = -7244512568000271500L;

    public DGraphException(String message, Throwable cause) {
        super(message, cause);
    }

    public DGraphException(String message) {
        super(message);
    }

}
