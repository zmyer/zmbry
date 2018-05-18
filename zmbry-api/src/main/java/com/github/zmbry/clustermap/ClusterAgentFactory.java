package com.github.zmbry.clustermap;

/**
 * @author zifeng
 *
 */
public interface ClusterAgentFactory {
    ClusterMap getClusterMap();

    ClusterParticipant getClusterParticipant();
}
