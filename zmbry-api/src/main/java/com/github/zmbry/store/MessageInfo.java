package com.github.zmbry.store;

import com.github.zmbry.utils.Utils;

/**
 * @author zifeng
 *
 */
public class MessageInfo {

    private final StoreKey key;
    private final long size;
    private final long expirationTimeInMs;
    private final boolean isDeleted;
    private final Long crc;
    private final short accountId;
    private final short containerId;
    private final long operationTimeMs;

    /**
     * Construct an instance of MessageInfo.
     * @param key the {@link StoreKey} associated with this message.
     * @param size the size of this message in bytes.
     * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
     * @param accountId accountId of the blob
     * @param containerId containerId of the blob
     * @param operationTimeMs operation time in ms
     */
    public MessageInfo(StoreKey key, long size, long expirationTimeInMs, short accountId, short containerId,
            long operationTimeMs) {
        this(key, size, false, expirationTimeInMs, accountId, containerId, operationTimeMs);
    }

    /**
     * Construct an instance of MessageInfo.
     * @param key the {@link StoreKey} associated with this message.
     * @param size the size of this message in bytes.
     * @param deleted {@code true} if the message is deleted, {@code false} otherwise
     * @param accountId accountId of the blob
     * @param containerId containerId of the blob
     * @param operationTimeMs operation time in ms
     */
    public MessageInfo(StoreKey key, long size, boolean deleted, short accountId, short containerId,
            long operationTimeMs) {
        this(key, size, deleted, Utils.Infinite_Time, accountId, containerId, operationTimeMs);
    }

    /**
     * Construct an instance of MessageInfo.
     * @param key the {@link StoreKey} associated with this message.
     * @param size the size of this message in bytes.
     * @param deleted {@code true} if the message is deleted, {@code false} otherwise
     * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
     * @param accountId accountId of the blob
     * @param containerId containerId of the blob
     * @param operationTimeMs operation time in ms
     */
    public MessageInfo(StoreKey key, long size, boolean deleted, long expirationTimeInMs, short accountId,
            short containerId, long operationTimeMs) {
        this(key, size, deleted, expirationTimeInMs, null, accountId, containerId, operationTimeMs);
    }

    /**
     * Construct an instance of MessageInfo.
     * @param key the {@link StoreKey} associated with this message.
     * @param size the size of this message in bytes.
     * @param containerId containerId of the blob
     * @param operationTimeMs operation time in ms
     */
    public MessageInfo(StoreKey key, long size, short accountId, short containerId, long operationTimeMs) {
        this(key, size, Utils.Infinite_Time, accountId, containerId, operationTimeMs);
    }

    /**
     * Construct an instance of MessageInfo.
     * @param key the {@link StoreKey} associated with this message.
     * @param size the size of this message in bytes.
     * @param deleted {@code true} if the message is deleted, {@code false} otherwise
     * @param expirationTimeInMs the time at which the message will expire. A value of -1 means no expiration.
     * @param crc the crc associated with this message. If unavailable, pass in null.
     * @param accountId accountId of the blob
     * @param containerId containerId of the blob
     * @param operationTimeMs operation time in ms
     */
    public MessageInfo(StoreKey key, long size, boolean deleted, long expirationTimeInMs, Long crc, short accountId,
            short containerId, long operationTimeMs) {
        if (operationTimeMs < Utils.Infinite_Time) {
            throw new IllegalArgumentException("OperationTime cannot be negative " + operationTimeMs);
        }
        this.key = key;
        this.size = size;
        this.isDeleted = deleted;
        this.expirationTimeInMs = expirationTimeInMs;
        this.crc = crc;
        this.accountId = accountId;
        this.containerId = containerId;
        this.operationTimeMs = operationTimeMs;
    }

    public StoreKey getStoreKey() {
        return key;
    }

    /**
     * Get size of message in bytes
     * @return size in bytes
     */
    public long getSize() {
        return size;
    }

    public long getExpirationTimeInMs() {
        return expirationTimeInMs;
    }

    public boolean isDeleted() {
        return isDeleted;
    }

    public boolean isExpired() {
        return getExpirationTimeInMs() != Utils.Infinite_Time && System.currentTimeMillis() > getExpirationTimeInMs();
    }

    /**
     * @return the crc associated with this message, if there is one; null otherwise.
     */
    public Long getCrc() {
        return crc;
    }

    public short getAccountId() {
        return accountId;
    }

    public short getContainerId() {
        return containerId;
    }

    public long getOperationTimeMs() {
        return operationTimeMs;
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("[MessageInfo:")
                .append("Key-")
                .append(key)
                .append(",")
                .append("Size-")
                .append(size)
                .append(",")
                .append("ExpirationTimeInMs-")
                .append(expirationTimeInMs)
                .append(",")
                .append("IsDeleted-")
                .append(isDeleted)
                .append(",")
                .append("Crc-")
                .append(crc)
                .append(",")
                .append("AccountId-")
                .append(accountId)
                .append(",")
                .append("ContainerId-")
                .append(containerId)
                .append(",")
                .append("OperationTimeMs-")
                .append(operationTimeMs)
                .append("]");
        return stringBuilder.toString();
    }
}
