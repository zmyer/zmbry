package com.github.zmbry.replication;

import com.github.zmbry.clustermap.DataNodeId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zifeng
 *
 */
public class DataNodeRemoteReplicaInfos {
    private Map<DataNodeId, List<RemoteReplicaInfo>> dataNodeToReplicaLists;

    public DataNodeRemoteReplicaInfos(final RemoteReplicaInfo remoteReplicaInfo) {
        this.dataNodeToReplicaLists = new HashMap<>();
        this.dataNodeToReplicaLists.put(remoteReplicaInfo.getReplicaId().getDataNodeId(), new ArrayList<>(Collections
                .singletonList(remoteReplicaInfo)));
    }

    public void addRemoteRelica(final RemoteReplicaInfo remoteReplicaInfo) {
        DataNodeId dataNodeIdToReplicate = remoteReplicaInfo.getReplicaId().getDataNodeId();
        List<RemoteReplicaInfo> replicaInfos = dataNodeToReplicaLists.get(dataNodeIdToReplicate);
        if (replicaInfos == null) {
            replicaInfos = new ArrayList<>();
        }
        replicaInfos.add(remoteReplicaInfo);
        dataNodeToReplicaLists.put(dataNodeIdToReplicate, replicaInfos);
    }

    public Set<DataNodeId> getDataNodeIds() {
        return dataNodeToReplicaLists.keySet();
    }

    public List<RemoteReplicaInfo> getRemoteReplicaListForDataNode(final DataNodeId dataNodeId) {
        return dataNodeToReplicaLists.get(dataNodeId);
    }
}
