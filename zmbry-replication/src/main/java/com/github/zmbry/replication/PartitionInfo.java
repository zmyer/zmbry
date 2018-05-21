package com.github.zmbry.replication;

import com.github.zmbry.clustermap.PartitionId;
import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.store.Store;

import java.util.List;

/**
 * @author zifeng
 *
 */
public class PartitionInfo {
    //远端分区副本信息集合
    private final List<RemoteReplicaInfo> remoteReplicas;
    //分区id
    private final PartitionId partitionId;
    //分区存储对象
    private final Store store;
    //本地副本id
    private final ReplicaId localReplicaId;

    public PartitionInfo(List<RemoteReplicaInfo> remoteReplicas, PartitionId partitionId, Store store,
            ReplicaId localReplicaId) {
        this.remoteReplicas = remoteReplicas;
        this.partitionId = partitionId;
        this.store = store;
        this.localReplicaId = localReplicaId;
    }

    public PartitionId getPartitionId() {
        return partitionId;
    }

    public List<RemoteReplicaInfo> getRemoteReplicaInfos() {
        return remoteReplicas;
    }

    public Store getStore() {
        return store;
    }

    public ReplicaId getLocalReplicaId() {
        return this.localReplicaId;
    }

    @Override
    public String toString() {
        return partitionId.toString() + " " + remoteReplicas.toString();
    }

}
