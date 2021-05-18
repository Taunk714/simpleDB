package simpledb;

import java.lang.Exception;
import java.util.HashMap;

/** Exception that is thrown when a transaction has aborted. */
public class TransactionAbortedException extends Exception {
    private static final long serialVersionUID = 1L;

    public TransactionAbortedException() {
    }
}
