package com.github.zmbry.network;

/**
 * @author zifeng
 *
 */
public class ConnectionPoolTimeoutException extends Exception {
    private static final long serialVersionUID = 1;

    public ConnectionPoolTimeoutException(String message) {
        super(message);
    }

    public ConnectionPoolTimeoutException(String message, Throwable e) {
        super(message, e);
    }

    public ConnectionPoolTimeoutException(Throwable e) {
        super(e);
    }
}
