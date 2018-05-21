package com.github.zmbry.network;

import com.github.zmbry.config.ConnectionPoolConfig;
import com.github.zmbry.config.SSLConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.net.SocketException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author zifeng
 *
 */
public class BlockingChannelInfo {
    private final ArrayBlockingQueue<BlockingChannel> mBlockingChannelAvailableConnections;
    private final ArrayBlockingQueue<BlockingChannel> mBlockingChannelActiveConnections;
    private final AtomicInteger numberofConnections;
    private final ConnectionPoolConfig mConnectionPoolConfig;
    private final ReadWriteLock mReadWriteLock;
    private final Object lock;
    private final String host;
    private final Port port;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private int maxConnectionPerHostPerPort;
    private final SSLSocketFactory mSSLSocketFactory;
    private final SSLConfig mSSLConfig;

    public BlockingChannelInfo(final ConnectionPoolConfig connectionPoolConfig, final String host, final Port port,
            final SSLSocketFactory sslSocketFactory, final SSLConfig sslConfig) {
        this.mConnectionPoolConfig = connectionPoolConfig;
        this.port = port;
        if (port.getPortType() == PortType.SSL) {
            maxConnectionPerHostPerPort = mConnectionPoolConfig.connectionPoolMaxConnectionsPerPortSSL;
        } else {
            maxConnectionPerHostPerPort = mConnectionPoolConfig.connectionPoolMaxConnectionsPerPortPlainText;
        }
        this.mBlockingChannelAvailableConnections = new ArrayBlockingQueue<BlockingChannel>(
                maxConnectionPerHostPerPort);
        this.mBlockingChannelActiveConnections = new ArrayBlockingQueue<BlockingChannel>(maxConnectionPerHostPerPort);
        this.numberofConnections = new AtomicInteger();
        this.mReadWriteLock = new ReentrantReadWriteLock();
        this.lock = new Object();
        this.host = host;
        this.mSSLSocketFactory = sslSocketFactory;
        this.mSSLConfig = sslConfig;
        logger.info("Starting blocking channel info for host {} and port {}", host, port.getPort());
    }

    public void releaseBlockingChannel(final BlockingChannel blockingChannel) {
        mReadWriteLock.readLock().lock();
        try {
            if (mBlockingChannelActiveConnections.remove(blockingChannel)) {
                mBlockingChannelAvailableConnections.add(blockingChannel);
                logger.trace(
                        "Adding connection to {}:{} back to pool. Current available connections {} Current active connections {}",
                        blockingChannel.getRemoteHost(), blockingChannel.getRemotePort(),
                        mBlockingChannelAvailableConnections.size(), mBlockingChannelActiveConnections.size());
            } else {
                logger.error(
                        "Tried to add invalid connection. Channel does not belong in the active queue. Host {} port {}"
                                + " channel host {} channel port {}", host, port.getPort(),
                        blockingChannel.getRemoteHost(),
                        blockingChannel.getRemotePort());
            }
        } finally {
            mReadWriteLock.readLock().unlock();
        }
    }

    public BlockingChannel getBlockingChannel(final long timeoutMs)
            throws ConnectionPoolTimeoutException, InterruptedException {
        mReadWriteLock.readLock().lock();
        try {
            if (numberofConnections.get() == maxConnectionPerHostPerPort || mBlockingChannelAvailableConnections.size
                    () > 0) {
                BlockingChannel blockingChannel = mBlockingChannelAvailableConnections.poll(timeoutMs, TimeUnit
                        .MILLISECONDS);
                if (blockingChannel != null) {
                    mBlockingChannelActiveConnections.add(blockingChannel);
                    logger.trace("Returning connection to " + blockingChannel.getRemoteHost() + ":" +
                            blockingChannel.getRemotePort());
                    //返回channel
                    return blockingChannel;
                } else if (numberofConnections.get() == maxConnectionPerHostPerPort) {
                    logger.error("Timed out trying to get a connection for host {} and port {}", host, port.getPort());
                    throw new ConnectionPoolTimeoutException(
                            "Could not get a connection to host " + host + " and port " + port.getPort());
                }
            }
            synchronized (lock) {
                if (numberofConnections.get() < maxConnectionPerHostPerPort) {
                    logger.trace("Planning to create a new connection for host {} and port {} ", host, port.getPort());
                    BlockingChannel blockingChannel = getBlockingChannelBasedOnPortType(host, port.getPort());
                    blockingChannel.connect();
                    numberofConnections.incrementAndGet();
                    logger.trace("Created a new connection for host {} and port {}. Number of connections {}", host,
                            port,
                            numberofConnections.get());
                    mBlockingChannelActiveConnections.add(blockingChannel);
                    return blockingChannel;
                }
            }
            BlockingChannel blockingChannel = mBlockingChannelAvailableConnections.poll(timeoutMs, TimeUnit
                    .MILLISECONDS);
            if (blockingChannel == null) {
                logger.error("Timed out trying to get a connection for host {} and port {}", host, port);
                throw new ConnectionPoolTimeoutException(
                        "Could not get a connection to host " + host + " and port " + port.getPort());
            }
            mBlockingChannelActiveConnections.add(blockingChannel);
            return blockingChannel;
        } catch (SocketException e) {
            logger.error("Socket exception when trying to connect to remote host {} and port {}", host, port.getPort());
            throw new ConnectionPoolTimeoutException(
                    "Socket exception when trying to connect to remote host " + host + " port " + port.getPort(), e);
        } catch (IOException e) {
            logger.error("IOException when trying to connect to the remote host {} and port {}", host, port.getPort());
            throw new ConnectionPoolTimeoutException(
                    "IOException when trying to connect to remote host " + host + " port " + port.getPort(), e);
        } finally {
            mReadWriteLock.readLock().unlock();
        }
    }

    private BlockingChannel getBlockingChannelBasedOnPortType(final String host, final int port) {
        BlockingChannel blockingChannel = null;
        if (this.port.getPortType() == PortType.PLAINTEXT) {
            blockingChannel = new BlockingChannel(host, port, mConnectionPoolConfig
                    .connectionPoolReadBufferSizeBytes, mConnectionPoolConfig.connectionPoolWriteBufferSizeBytes,
                    mConnectionPoolConfig.connectionPoolReadTimeoutMs, mConnectionPoolConfig
                    .connectionPoolConnectTimeoutMs);
        } else if (this.port.getPortType() == PortType.SSL) {
            blockingChannel = new SSLBlockingChannel(host, port, mConnectionPoolConfig
                    .connectionPoolReadBufferSizeBytes, mConnectionPoolConfig.connectionPoolWriteBufferSizeBytes,
                    mConnectionPoolConfig.connectionPoolReadTimeoutMs, mConnectionPoolConfig
                    .connectionPoolConnectTimeoutMs, mSSLSocketFactory, mSSLConfig);
        }
        return blockingChannel;
    }

    public void destroyBlockingChannel(final BlockingChannel blockingChannel) {
        mReadWriteLock.readLock().lock();
        try {
            boolean changed = mBlockingChannelActiveConnections.remove(blockingChannel);
            if (!changed) {
                logger.error("Invalid connection being destroyed. "
                                + "Channel does not belong to this queue. queue host {} port {} channel host {} port {}", host,
                        port.getPort(), blockingChannel.getRemoteHost(), blockingChannel.getRemotePort());
                throw new IllegalArgumentException("Invalid connection. Channel does not belong to this queue");
            }
            blockingChannel.disconnect();
            BlockingChannel channel = getBlockingChannelBasedOnPortType(blockingChannel.getRemoteHost(),
                    blockingChannel.getRemotePort());
            channel.connect();
            logger.trace("Destroying connection and adding new connection for host {} port {}", host, port.getPort());
            mBlockingChannelAvailableConnections.add(channel);
        } catch (Exception e) {
            logger.error(
                    "Connection failure to remote host {} and port {} when destroying and recreating the connection",
                    host, port.getPort());
            synchronized (lock) {
                // decrement the number of connections to the host and port. we were not able to maintain the count
                numberofConnections.decrementAndGet();
                // at this point we are good to clean up the available connections since re-creation failed
                do {
                    //清空所有的连接？
                    BlockingChannel channel = mBlockingChannelAvailableConnections.poll();
                    if (channel == null) {
                        break;
                    }
                    //断开连接
                    channel.disconnect();
                    numberofConnections.decrementAndGet();
                } while (true);
            }
        } finally {
            mReadWriteLock.readLock().unlock();
        }
    }

    public int getNumberOfConnections() {
        return this.numberofConnections.intValue();
    }

    public void cleanup() {
        mReadWriteLock.writeLock().lock();
        logger.info("Cleaning all active and available connections for host {} and port {}", host, port.getPort());
        try {
            for (BlockingChannel channel : mBlockingChannelActiveConnections) {
                //断开连接
                channel.disconnect();
            }
            mBlockingChannelActiveConnections.clear();
            for (BlockingChannel channel : mBlockingChannelAvailableConnections) {
                //断开连接
                channel.disconnect();
            }
            mBlockingChannelAvailableConnections.clear();
            numberofConnections.set(0);
            logger.info("Cleaning completed for all active and available connections for host {} and port {}", host,
                    port.getPort());
        } finally {
            mReadWriteLock.writeLock().unlock();
        }
    }
}
