package com.github.zmbry.network;

/**
 * @author zifeng
 *
 */
public interface ConnectionPool {
    void start();

    void shutDown();

    ConnectedChannel checkOutConnection(final String host, final Port port, final long timeout)
            throws InterruptedException, ConnectionPoolTimeoutException;

    void checkInConnection(final ConnectedChannel connectedChannel);

    void destroyConnection(final ConnectedChannel connectedChannel);
}
