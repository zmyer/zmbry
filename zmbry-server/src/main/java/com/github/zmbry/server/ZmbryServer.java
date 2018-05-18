package com.github.zmbry.server;

import com.github.zmbry.clustermap.ClusterAgentFactory;
import com.github.zmbry.clustermap.ClusterMap;
import com.github.zmbry.clustermap.ClusterParticipant;
import com.github.zmbry.clustermap.DataNodeId;
import com.github.zmbry.config.ClusterMapConfig;
import com.github.zmbry.config.ConnectionPoolConfig;
import com.github.zmbry.config.DiskManagerConfig;
import com.github.zmbry.config.NetworkConfig;
import com.github.zmbry.config.ReplicationConfig;
import com.github.zmbry.config.SSLConfig;
import com.github.zmbry.config.ServerConfig;
import com.github.zmbry.config.StatsManagerConfig;
import com.github.zmbry.config.StoreConfig;
import com.github.zmbry.config.VerifiableProperties;
import com.github.zmbry.store.FindTokenFactory;
import com.github.zmbry.store.StoreKeyFactory;
import com.github.zmbry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.NetworkServer;

import java.lang.reflect.InvocationTargetException;
import java.sql.Time;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author zifeng
 *
 */
public class ZmbryServer {
    private Logger logger = LoggerFactory.getLogger(getClass());
    private CountDownLatch mShutDownLatch = new CountDownLatch(1);
    private NetworkServer mNetworkServer = null;
    private ZmbryRequests mRequests = null;
    private RequestHandlerPool mRequestHandlerPool = null;
    private ScheduledExecutorService mScheduledExecutorService;
    private StorageManager mStorageManager = null;
    private StatsManager mStatsManager = null;
    private ReplicationManager mReplicationManager = null;
    private VerifiableProperties mVerifableProperties;
    private ClusterAgentFactory mClusterAgentFactory;
    private ClusterMap mClusterMap;
    private ClusterParticipant mClusterParticipant;
    private ConnectionPool mConnectionPool = null;
    private NotificationSystem mNotificationSystem;
    private Time mTime;

    public ZmbryServer(final VerifiableProperties properties, final ClusterAgentFactory clusterAgentFactory, final Time
            time) {
        this(properties, clusterAgentFactory, new LoggerNotificationSystem(), time);
    }

    public ZmbryServer(VerifiableProperties properties, ClusterAgentFactory clusterAgentFactory, NotificationSystem
            notificationSystem, Time time) {
        this.mVerifableProperties = properties;
        this.mClusterAgentFactory = clusterAgentFactory;
        this.mNotificationSystem = notificationSystem;
        this.mTime = time;
    }

    public void startUp()
            throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException,
            IllegalAccessException {
        logger.info("starting");
        mClusterMap = mClusterAgentFactory.getClusterMap();
        logger.info("Initialized custerMap");
        mClusterParticipant = mClusterAgentFactory.getClusterParticipant();

        logger.info("creating configs");
        NetworkConfig netWorkConfig = new NetworkConfig(mVerifableProperties);
        StoreConfig storeConfig = new StoreConfig(mVerifableProperties);
        DiskManagerConfig diskManagerConfig = new DiskManagerConfig(mVerifableProperties);
        ServerConfig serverConfig = new ServerConfig(mVerifableProperties);
        ReplicationConfig replicationConfig = new ReplicationConfig(mVerifableProperties);
        ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig(mVerifableProperties);
        SSLConfig sslConfig = new SSLConfig(mVerifableProperties);
        ClusterMapConfig clusterMapConfig = new ClusterMapConfig(mVerifableProperties);
        StatsManagerConfig statsManagerConfig = new StatsManagerConfig(mVerifableProperties);
        mVerifableProperties.verify();

        mScheduledExecutorService = Utils.newScheduler(serverConfig.serverRequestHandlerNumOfThreads, false);
        logger.info("check node exist");

        DataNodeId nodeId = mClusterMap.getDataNodeId(netWorkConfig.hostName, netWorkConfig.port);
        if (nodeId == null) {
            throw new IllegalArgumentException("The node " + netWorkConfig.hostName + ":" + netWorkConfig.port
                    + "is not present in the clustermap. Failing to start the datanode");
        }

        StoreKeyFactory storeKeyFactory = Utils.getObj(storeConfig.storeKeyFactory, mClusterMap);
        FindTokenFactory findTokenFactory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);

        mStorageManager = new StorageManager(storeConfig, diskManagerConfig, mScheduledExecutorService, mClusterMap
                .getReplicaIds(nodeId), storeKeyFactory, new BlobStoreRecovery(), new BlockStoreHardDelete());






















    }
}
