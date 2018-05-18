package com.github.zmbry.clustermap;

/**
 * @author zifeng
 *
 */
public interface DiskId extends Resource {
    String getMountPath();

    HardwareState getState();

    long getRawCapacityInBytes();
}
