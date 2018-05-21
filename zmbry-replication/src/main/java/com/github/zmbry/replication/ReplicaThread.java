package com.github.zmbry.replication;

import com.github.zmbry.clustermap.ClusterMap;
import com.github.zmbry.clustermap.DataNodeId;
import com.github.zmbry.clustermap.PartitionId;
import com.github.zmbry.config.ReplicationConfig;
import com.github.zmbry.network.ConnectionPool;
import com.github.zmbry.notification.NotificationSystem;
import com.github.zmbry.store.FindTokenFactory;
import com.github.zmbry.store.StoreKeyFactory;
import org.omg.CORBA.portable.ResponseHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zifeng
 *
 */
public class ReplicaThread implements Runnable {
    private final Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateGroupedByNode;
    private final Set<PartitionId> replicationDisablePartitions = new HashSet<>();
    private final Set<PartitionId> unmodifiableReplicationDisabledPartitions =
            Collections.unmodifiableSet(replicationDisablePartitions);
    private final Set<PartitionId> allReplicatedPartitions;
    private final CountDownLatch shutDownLatch = new java.util.concurrent.CountDownLatch(1);
    private volatile boolean running;
    private boolean waitEnabled;
    private final FindTokenFactory mFindTokenFactory;
    private final ClusterMap mClusterMap;
    private final AtomicInteger correlationIdGenerator;
    private final DataNodeId mDataNodeId;
    private final ConnectionPool mConnectionPool;
    private final ReplicationConfig mReplicationConfig;
    private final String threadName;
    private final NotificationSystem mNotificationSystem;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final StoreKeyFactory mStoreKeyFactory;
    private final boolean validateMessageStream;
    private final ResponseHandler mResponseHandler;
    private final boolean replicatingFromRemoteColo;
    private final boolean replicationOverSsl;
    private final String datacenterName;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition pauseCondition = lock.newCondition();
    private volatile boolean allDisabled = false;

    ReplicaThread(String threadName, Map<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateGroupedByNode,
            FindTokenFactory findTokenFactory, ClusterMap clusterMap, AtomicInteger correlationIdGenerator,
            DataNodeId dataNodeId, ConnectionPool connectionPool, ReplicationConfig replicationConfig,
            NotificationSystem notification, StoreKeyFactory storeKeyFactory,
            boolean validateMessageStream, boolean replicatingOverSsl,
            String datacenterName,
            ResponseHandler responseHandler) {
        this.threadName = threadName;
        this.replicasToReplicateGroupedByNode = replicasToReplicateGroupedByNode;
        this.running = true;
        this.mFindTokenFactory = findTokenFactory;
        this.mClusterMap = clusterMap;
        this.correlationIdGenerator = correlationIdGenerator;
        this.mDataNodeId = dataNodeId;
        this.mConnectionPool = connectionPool;
        this.mReplicationConfig = replicationConfig;
        this.mNotificationSystem = notification;
        this.mStoreKeyFactory = storeKeyFactory;
        this.validateMessageStream = validateMessageStream;
        this.mResponseHandler = responseHandler;
        this.replicatingFromRemoteColo = !(dataNodeId.getDataCenterName().equals(datacenterName));
        this.waitEnabled = !replicatingFromRemoteColo;
        this.replicationOverSsl = replicatingOverSsl;
        this.datacenterName = datacenterName;
        Set<PartitionId> partitions = new HashSet<>();
        for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> entry : replicasToReplicateGroupedByNode.entrySet()) {
            for (RemoteReplicaInfo info : entry.getValue()) {
                partitions.add(info.getReplicaId().getPartitionId());
            }
        }
        allReplicatedPartitions = Collections.unmodifiableSet(partitions);
    }

    @Override
    public void run() {
        try {
            List<List<RemoteReplicaInfo>> replicasToReplicate = new ArrayList<>(replicasToReplicateGroupedByNode.size
                    ());
            for (Map.Entry<DataNodeId, List<RemoteReplicaInfo>> replicasToReplicateEntry :
                    replicasToReplicateGroupedByNode.entrySet()) {
                replicasToReplicate.add(replicasToReplicateEntry.getValue());
            }
            logger.info("Begin iteration for thread " + threadName);
            while (running) {
                replicate(replicasToReplicate);
                lock.lock();
                try {
                    if (running && allDisabled) {
                        pauseCondition.await();
                    }
                } catch (Exception e) {
                    logger.error("Received interrupted exception during pause", e);

                } finally {
                    lock.unlock();
                }
            }
        } finally {
            running = false;
            shutDownLatch.countDown();
        }
    }

    private void replicate(final List<List<RemoteReplicaInfo>> replicasToReplicate) {
        Collections.shuffle(replicasToReplicate);
        for(List<RemoteReplicaInfo> replicasToReplicatePerNode : replicasToReplicate) {
            if(!running) {
                break;
            }
            DataNodeId remoteNode = replicasToReplicatePerNode.get(0).getReplicaId().getDataNodeId();
            logger.trace("Remote node: {} Thread name: {} Remote replicas: {}", remoteNode, threadName,
                    replicasToReplicatePerNode);

        }
    }
}
