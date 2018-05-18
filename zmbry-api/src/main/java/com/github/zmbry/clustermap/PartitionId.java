package com.github.zmbry.clustermap;

import java.util.List;

/**
 * @author zifeng
 *
 */
public abstract class PartitionId implements Resource, Comparable<PartitionId> {
    public abstract byte[] getBytes();

    public abstract List<? extends ReplicaId> getReplicaIds();

    public abstract PartitionState getPartitionState();

    public abstract boolean isEqual(String partitionId);

    @Override
    public abstract String toString();

    public abstract String toPathString();
}
