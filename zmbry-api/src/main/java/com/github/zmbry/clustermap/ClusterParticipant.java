package com.github.zmbry.clustermap;

import com.github.zmbry.server.ZmbryHealthReport;

import java.util.List;

/**
 * @author zifeng
 *
 */
public interface ClusterParticipant extends AutoCloseable {
    void initialize(String hostName, int port, List<ZmbryHealthReport> zmbryHealthReportList);

    boolean setReplicaSealedState(ReplicaId replicaId, boolean isSealed);

    @Override
    void close();
}
