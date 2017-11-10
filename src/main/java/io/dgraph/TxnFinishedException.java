package io.dgraph;

public class TxnFinishedException extends TxnException {
  TxnFinishedException() {
    super("Transaction has already been committed or discarded");
  }
}
