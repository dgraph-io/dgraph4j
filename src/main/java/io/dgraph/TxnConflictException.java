package io.dgraph;

public class TxnConflictException extends TxnException {
  public TxnConflictException(String msg) {
    super(msg);
  }
}
