package io.dgraph;

public class TxnException extends RuntimeException {

  public TxnException(String message) {
    super(message);
  }

  public TxnException(String message, Throwable cause) {
    super(message, cause);
  }
}
