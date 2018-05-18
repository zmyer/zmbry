package com.github.zmbry.clustermap;

import java.util.List;

/**
 * @author zifeng
 *
 */
public interface ReplicaId {
    PartitionId getPartition();

    DataNodeId getDataNodeId();

    String getMountPath();

    String getReplicaPath();

    List<? extends ReplicaId> getPeerReplicaIds();

    long getCapacityInBytes();

    DiskId getDiskId();

    boolean isDown();

    boolean isSealed();
}
