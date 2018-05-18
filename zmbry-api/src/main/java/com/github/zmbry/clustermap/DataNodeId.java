package com.github.zmbry.clustermap;

import com.github.zmbry.network.Port;

/**
 * @author zifeng
 *
 */
public abstract class DataNodeId implements Resource, Comparable<DataNodeId> {
    public abstract String getHostName();

    public abstract int getPort();

    public abstract int getSSLPort();

    public abstract boolean hasSSLPort();

    public abstract Port getPortToConnectTo();

    public abstract HardwareState getState();

    public abstract String getDataCenterName();

    public abstract long getRackId();
}
