package com.github.zmbry.clustermap;

/**
 * @author zifeng
 *
 */
public enum ReplicaEventType {
    NODE_RESPONSE,
    NODE_TIMEOUT,
    DISK_ERROR,
    DISK_OK,
    PARTITION_READONLY
}
