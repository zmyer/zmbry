package com.github.zmbry.messageformat;

import com.github.zmbry.utils.SystemTime;
import com.github.zmbry.utils.Utils;

/**
 * @author zifeng
 *
 */
public class BlobProperties {
    private final long blobSize;
    private final String serviceId;
    private final String ownerId;
    private final String contentType;
    private final boolean isPrivate;
    private final long timeToLiveInSeconds;
    private final long creationTimeInMs;
    private final short accountId;
    private final short containerId;
    private final boolean isEncrypted;

    /**
     * @param blobSize The size of the blob in bytes
     * @param serviceId The service id that is creating this blob
     * @param accountId accountId of the user who owns the blob
     * @param containerId containerId of the blob
     * @param isEncrypted {@code true} if the blob is encrypted, {@code false} otherwise
     */
    public BlobProperties(long blobSize, String serviceId, short accountId, short containerId, boolean isEncrypted) {
        this(blobSize, serviceId, null, null, false, Utils.Infinite_Time, SystemTime.getInstance().milliseconds(),
                accountId, containerId, isEncrypted);
    }

    /**
     * @param blobSize The size of the blob in bytes
     * @param serviceId The service id that is creating this blob
     * @param ownerId The owner of the blob (For example , memberId or groupId)
     * @param contentType The content type of the blob (eg: mime). Can be Null
     * @param isPrivate Is the blob secure
     * @param timeToLiveInSeconds The time to live, in seconds, relative to blob creation time.
     * @param accountId accountId of the user who owns the blob
     * @param containerId containerId of the blob
     * @param isEncrypted whether this blob is encrypted.
     */
    public BlobProperties(long blobSize, String serviceId, String ownerId, String contentType, boolean isPrivate,
            long timeToLiveInSeconds, short accountId, short containerId, boolean isEncrypted) {
        this(blobSize, serviceId, ownerId, contentType, isPrivate, timeToLiveInSeconds,
                SystemTime.getInstance().milliseconds(), accountId, containerId, isEncrypted);
    }

    /**
     * @param blobSize The size of the blob in bytes
     * @param serviceId The service id that is creating this blob
     * @param ownerId The owner of the blob (For example , memberId or groupId)
     * @param contentType The content type of the blob (eg: mime). Can be Null
     * @param isPrivate Is the blob secure
     * @param timeToLiveInSeconds The time to live, in seconds, relative to blob creation time.
     * @param creationTimeInMs The time at which the blob is created.
     * @param accountId accountId of the user who owns the blob
     * @param containerId containerId of the blob
     * @param isEncrypted whether this blob is encrypted.
     */
    public BlobProperties(long blobSize, String serviceId, String ownerId, String contentType, boolean isPrivate,
            long timeToLiveInSeconds, long creationTimeInMs, short accountId, short containerId, boolean isEncrypted) {
        this.blobSize = blobSize;
        this.serviceId = serviceId;
        this.ownerId = ownerId;
        this.contentType = contentType;
        this.isPrivate = isPrivate;
        this.creationTimeInMs = creationTimeInMs;
        this.timeToLiveInSeconds = timeToLiveInSeconds;
        this.accountId = accountId;
        this.containerId = containerId;
        this.isEncrypted = isEncrypted;
    }

    public long getTimeToLiveInSeconds() {
        return timeToLiveInSeconds;
    }

    public long getBlobSize() {
        return blobSize;
    }

    /**
     * This should only be used to determine the "virtual container" for blobs with V1 IDs.
     * @return whether the blob was private at creation time.
     */
    @Deprecated
    public boolean isPrivate() {
        return isPrivate;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public String getContentType() {
        return contentType;
    }

    /**
     * ServiceId of the uploader of the blob
     * @return the serviceId of the uploader of the blob
     */
    public String getServiceId() {
        return serviceId;
    }

    public long getCreationTimeInMs() {
        return creationTimeInMs;
    }

    public short getAccountId() {
        return accountId;
    }

    public short getContainerId() {
        return containerId;
    }

    public boolean isEncrypted() {
        return isEncrypted;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BlobProperties[");
        sb.append("BlobSize=").append(getBlobSize());
        if (getContentType() != null) {
            sb.append(", ").append("ContentType=").append(getContentType());
        } else {
            sb.append(", ").append("ContentType=Null");
        }
        if (getOwnerId() != null) {
            sb.append(", ").append("OwnerId=").append(getOwnerId());
        } else {
            sb.append(", ").append("OwnerId=Null");
        }
        if (getServiceId() != null) {
            sb.append(", ").append("ServiceId=").append(getServiceId());
        } else {
            sb.append(", ").append("ServiceId=Null");
        }
        sb.append(", ").append("IsPrivate=").append(isPrivate());
        sb.append(", ").append("CreationTimeInMs=").append(getCreationTimeInMs());
        if (getTimeToLiveInSeconds() != Utils.Infinite_Time) {
            sb.append(", ").append("TimeToLiveInSeconds=").append(getTimeToLiveInSeconds());
        } else {
            sb.append(", ").append("TimeToLiveInSeconds=Infinite");
        }
        sb.append(", ").append("AccountId=").append(getAccountId());
        sb.append(", ").append("ContainerId=").append(getContainerId());
        sb.append(", ").append("IsEncrypted=").append(isEncrypted());
        sb.append("]");
        return sb.toString();
    }

}
