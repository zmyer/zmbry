package com.github.zmbry.store;

/**
 * @author zifeng
 *
 */
public class StoreException extends Exception {
    private static final long serialVersionUID = 1;
    private final StoreErrorCodes error;

    public StoreException(String message, StoreErrorCodes error) {
        super(message);
        this.error = error;
    }

    public StoreException(String message, Throwable e, StoreErrorCodes error) {
        super(message, e);
        this.error = error;
    }

    public StoreException(Throwable e, StoreErrorCodes error) {
        super(e);
        this.error = error;
    }

    public StoreErrorCodes getErrorCode() {
        return this.error;
    }
}
