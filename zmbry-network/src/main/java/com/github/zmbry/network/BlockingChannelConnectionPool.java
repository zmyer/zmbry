package com.github.zmbry.network;

import com.github.zmbry.commons.SSLFactory;
import com.github.zmbry.config.ClusterMapConfig;
import com.github.zmbry.config.ConnectionPoolConfig;
import com.github.zmbry.config.SSLConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zifeng
 *
 */
public class BlockingChannelConnectionPool implements ConnectionPool {
    private final Map<String, BlockingChannelInfo> connections;
    private final ConnectionPoolConfig mConnectionPoolConfig;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final AtomicInteger requestsWaitingToCheckoutConnectionCount;
    private SSLSocketFactory mSSLSocketFactory;
    private final SSLConfig mSSLConfig;

    public BlockingChannelConnectionPool(final ConnectionPoolConfig connectionPoolConfig, final SSLConfig sslConfig,
            final ClusterMapConfig clusterMapConfig) throws GeneralSecurityException, IOException {
        connections = new ConcurrentHashMap<>();
        this.mConnectionPoolConfig = connectionPoolConfig;
        this.mSSLConfig = sslConfig;
        requestsWaitingToCheckoutConnectionCount = new AtomicInteger(0);
        if (clusterMapConfig.clusterMapSslEnabledDatacenters.length() > 0) {
            initializeSSLSocketFactory();
        } else {
            mSSLSocketFactory = null;
        }
    }

    private void initializeSSLSocketFactory() throws GeneralSecurityException, IOException {
        try {
            SSLFactory sslFactory = new SSLFactory(mSSLConfig);
            SSLContext sslContext = sslFactory.getSSLContext();
            this.mSSLSocketFactory = sslContext.getSocketFactory();
        } catch (Exception e) {
            logger.error("SSLSocketFactory Client Initialization Error ", e);
            throw e;
        }
    }

    @Override
    public void start() {
        logger.info("BlockingChannelConnectionPool started");
    }

    @Override
    public void shutDown() {
        logger.info("Shutting down the BlockingChannelConnectionPool");
        for (Map.Entry<String, BlockingChannelInfo> channels : connections.entrySet()) {
            channels.getValue().cleanup();
        }
    }

    @Override
    public ConnectedChannel checkOutConnection(final String host, final Port port, final long timeout)
            throws InterruptedException, ConnectionPoolTimeoutException {
        try {
            requestsWaitingToCheckoutConnectionCount.incrementAndGet();
            //根据提供的连接信息，查找具体的通道信息
            BlockingChannelInfo blockingChannelInfo = connections.get(host + port.getPort());
            if (blockingChannelInfo == null) {
                synchronized (this) {
                    //获取连接信息
                    blockingChannelInfo = connections.get(host + port.getPort());
                    if (blockingChannelInfo == null) {
                        logger.trace("Creating new blocking channel info for host {} and port {}", host,
                                port.getPort());
                        //如果当前的连接不存在，则直接创建
                        blockingChannelInfo = new BlockingChannelInfo(mConnectionPoolConfig, host, port,
                                mSSLSocketFactory,
                                mSSLConfig);
                        //新创建的连接插入到集合中
                        connections.put(host + port.getPort(), blockingChannelInfo);
                    } else {
                        logger.trace("Using already existing BlockingChannelInfo for " + host + ":" + port.getPort()
                                + " in synchronized block");
                    }
                }
            } else {
                logger.trace("Using already existing BlockingChannelInfo for " + host + ":" + port.getPort());
            }
            //返回对应的通道对象
            return blockingChannelInfo.getBlockingChannel(timeout);
        } finally {
            requestsWaitingToCheckoutConnectionCount.decrementAndGet();
        }
    }

    @Override
    public void checkInConnection(final ConnectedChannel connectedChannel) {
        try {
            //读取channel信息
            BlockingChannelInfo blockingChannelInfo =
                    connections.get(connectedChannel.getRemoteHost() + connectedChannel.getRemotePort());
            if (blockingChannelInfo == null) {
                logger.error("Unexpected state in connection pool. Host {} and port {} not found to checkin connection",
                        connectedChannel.getRemoteHost(), connectedChannel.getRemotePort());
                throw new IllegalArgumentException("Connection does not belong to the pool");
            }
            //释放掉指定的channel
            blockingChannelInfo.releaseBlockingChannel((BlockingChannel) connectedChannel);
            logger.trace("Checking in connection for host {} and port {}", connectedChannel.getRemoteHost(),
                    connectedChannel.getRemotePort());
        } finally {
        }

    }

    @Override
    public void destroyConnection(final ConnectedChannel connectedChannel) {
        try {
            //获取channel信息
            BlockingChannelInfo blockingChannelInfo =
                    connections.get(connectedChannel.getRemoteHost() + connectedChannel.getRemotePort());
            if (blockingChannelInfo == null) {
                logger.error("Unexpected state in connection pool. Host {} and port {} not found to checkin connection",
                        connectedChannel.getRemoteHost(), connectedChannel.getRemotePort());
                throw new IllegalArgumentException("Connection does not belong to the pool");
            }
            //销毁指定的channel对象
            blockingChannelInfo.destroyBlockingChannel((BlockingChannel) connectedChannel);
            logger.trace("Destroying connection for host {} and port {}", connectedChannel.getRemoteHost(),
                    connectedChannel.getRemotePort());
        } finally {
        }
    }
}
