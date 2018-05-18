package com.github.zmbry.store;

/**
 * @author zifeng
 *
 */
public abstract class StoreKey implements Comparable<StoreKey> {
    public abstract byte[] toBytes();

    public abstract short sizeInBytes();

    public abstract String getID();

    public abstract short getAccountId();

    public abstract short getContainerId();

    public abstract String getLongForm();
}
