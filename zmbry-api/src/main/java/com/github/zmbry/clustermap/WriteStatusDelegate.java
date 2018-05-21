package com.github.zmbry.clustermap;

/**
 * @author zifeng
 *
 */
public class WriteStatusDelegate {
    private final ClusterParticipant mClusterParticipant;

    public WriteStatusDelegate(final ClusterParticipant clusterParticipant) {
        this.mClusterParticipant = clusterParticipant;
    }

    public boolean seal(final ReplicaId replicaId) {
        return mClusterParticipant.setReplicaSealedState(replicaId, true);
    }

    public boolean unseal(final ReplicaId replicaId) {
        return mClusterParticipant.setReplicaSealedState(replicaId, false);
    }
}
