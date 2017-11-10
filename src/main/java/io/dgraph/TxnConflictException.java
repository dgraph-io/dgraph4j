package io.dgraph;

public class TxnConflictException extends TxnException {
  TxnConflictException() {
    super("Transaction has been aborted due to conflict");
  }
}
