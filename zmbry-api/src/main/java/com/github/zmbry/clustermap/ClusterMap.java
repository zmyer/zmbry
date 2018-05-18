package com.github.zmbry.clustermap;

import java.io.InputStream;
import java.util.List;

/**
 * @author zifeng
 *
 */
public interface ClusterMap extends AutoCloseable {

    PartitionId getPartitionIdFromStream(InputStream inputStream);

    List<? extends PartitionId> getWritablePartitionIds();

    List<? extends PartitionId> getAllPartitionIds();

    boolean hasDataCenter(String dataCenterName);

    byte getLocalDataCenterId();

    DataNodeId getDataNodeId(String hostName, int port);

    List<? extends ReplicaId> getReplicaIds(DataNodeId dataNodeId);

    List<? extends DataNodeId> getDataNodeIds();

    void onRelicaEvent(ReplicaId replicaId, ReplicaEventType replicaEventType);

    void closee();
}
