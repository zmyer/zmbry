package com.github.zmbry.replication;

import com.github.zmbry.clustermap.ClusterMap;
import com.github.zmbry.clustermap.DataNodeId;
import com.github.zmbry.clustermap.PartitionId;
import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.config.ClusterMapConfig;
import com.github.zmbry.config.ReplicationConfig;
import com.github.zmbry.config.StoreConfig;
import com.github.zmbry.network.ConnectionPool;
import com.github.zmbry.notification.NotificationSystem;
import com.github.zmbry.store.FindTokenFactory;
import com.github.zmbry.store.StorageManager;
import com.github.zmbry.store.Store;
import com.github.zmbry.store.StoreKeyFactory;
import com.github.zmbry.utils.SystemTime;
import com.github.zmbry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zifeng
 *
 */
public class ReplicationManager {
    private final Map<PartitionId, PartitionInfo> mPartitionsToReplicate;
    private final Map<String, List<PartitionInfo>> mPartitionGroupedByMountPath;
    private final ReplicationConfig mReplicationConfig;
    private final FindTokenFactory mFindTokenFactory;
    private final ClusterMap mClusterMap;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final ReplicaTokenPersistor mReplicaTokenPersistor;
    private final ScheduledExecutorService mScheduledExecutorService;
    private final AtomicInteger correlationIdGenerator;
    private final DataNodeId mDataNodeId;
    private final ConnectionPool mConnectionPool;
    private final NotificationSystem mNotificationSystem;
    private final Map<String, DataNodeRemoteReplicaInfos> mDataNodeRemoteReplicaInfosMap;
    private final StoreKeyFactory mStoreKeyFactory;
    private final List<String> sslEnabledDataCenters;
    private final Map<String, List<ReplicaThread>> replicaThreadPools;
    private final Map<String, Integer> numberOfReplicaThreads;

    private static final String replicaTokenFileName = "replicaTokens";
    private static final short Crc_Size = 8;
    private static final short Replication_Delay_Multiplier = 5;

    public ReplicationManager(ReplicationConfig replicationConfig, ClusterMapConfig clusterMapConfig,
            StoreConfig storeConfig, StorageManager storageManager, StoreKeyFactory storeKeyFactory,
            ClusterMap clusterMap, ScheduledExecutorService scheduler, DataNodeId dataNode,
            ConnectionPool connectionPool, NotificationSystem requestNotification) throws ReplicationException {

        try {
            this.mReplicationConfig = replicationConfig;
            this.mStoreKeyFactory = storeKeyFactory;
            this.mFindTokenFactory = Utils.getObj(replicationConfig.replicationTokenFactory, storeKeyFactory);
            this.replicaThreadPools = new HashMap<>();
            this.mPartitionGroupedByMountPath = new HashMap<>();
            this.mPartitionsToReplicate = new HashMap<>();
            this.mClusterMap = clusterMap;
            this.mScheduledExecutorService = scheduler;
            this.mReplicaTokenPersistor = new ReplicaTokenPersistor();
            this.correlationIdGenerator = new AtomicInteger(0);
            this.mDataNodeId = dataNode;
            List<? extends ReplicaId> replicaIds = clusterMap.getReplicaIds(mDataNodeId);
            this.mConnectionPool = connectionPool;
            this.mNotificationSystem = requestNotification;
            this.mDataNodeRemoteReplicaInfosMap = new HashMap<>();
            this.sslEnabledDataCenters = Utils.splitString(clusterMapConfig.clusterMapSslEnabledDatacenters, ",");
            this.numberOfReplicaThreads = new HashMap<>();

            // initialize all partitions
            for (ReplicaId replicaId : replicaIds) {
                //遍历每个副本，并获取副本对应的分区id
                PartitionId partition = replicaId.getPartitionId();
                //根据分区id,读取存储对象
                Store store = storageManager.getStore(partition);
                if (store != null) {
                    //根据副本id，获取存储在其他节点上的副本id
                    List<? extends ReplicaId> peerReplicas = replicaId.getPeerReplicaIds();
                    if (peerReplicas != null) {
                        //结果集
                        List<RemoteReplicaInfo> remoteReplicas = new ArrayList<RemoteReplicaInfo>(peerReplicas.size());
                        for (ReplicaId remoteReplica : peerReplicas) {
                            // We need to ensure that a replica token gets persisted only after the corresponding data in the
                            // store gets flushed to disk. We use the store flush interval multiplied by a constant factor
                            // to determine the token flush interval
                            //创建远端副本信息对象
                            RemoteReplicaInfo remoteReplicaInfo =
                                    new RemoteReplicaInfo(remoteReplica, replicaId, store,
                                            mFindTokenFactory.getNewFindToken(),
                                            storeConfig.storeDataFlushIntervalSeconds * SystemTime.MsPerSec *
                                                    Replication_Delay_Multiplier,
                                            SystemTime.getInstance(),
                                            remoteReplica.getDataNodeId().getPortToConnectTo());
                            //插入结果集
                            remoteReplicas.add(remoteReplicaInfo);
                            //更新副本信息
                            updateReplicasToReplicate(remoteReplica.getDataNodeId().getDataCenterName(),
                                    remoteReplicaInfo);
                        }
                        //创建分区信息对象
                        PartitionInfo partitionInfo = new PartitionInfo(remoteReplicas, partition, store, replicaId);
                        //将分区对应的副本信息插入到集合中
                        mPartitionsToReplicate.put(partition, partitionInfo);
                        //根据副本的挂载点，读取分区信息集合
                        List<PartitionInfo> partitionInfos = mPartitionGroupedByMountPath.get(replicaId.getMountPath());
                        if (partitionInfos == null) {
                            //不存在，则新建
                            partitionInfos = new ArrayList<>();
                        }
                        //将分区信息插入到集合中
                        partitionInfos.add(partitionInfo);
                        //将分区挂载点与分区对应关系插入到集合中
                        mPartitionGroupedByMountPath.put(replicaId.getMountPath(), partitionInfos);
                    }
                } else {
                    logger.error(
                            "Not replicating to partition " + partition +
                                    " because an initialized store could not be found");
                }
            }
        } catch (Exception e) {
            logger.error("Error on starting replication manager", e);
            throw new ReplicationException("Error on starting replication manager");
        }
    }

    private void updateReplicasToReplicate(final String dataCenterName, final RemoteReplicaInfo remoteReplicaInfo) {
    }

    public void start() {
    }
}
