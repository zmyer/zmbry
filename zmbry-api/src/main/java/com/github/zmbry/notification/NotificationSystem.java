package com.github.zmbry.notification;

import com.github.zmbry.messageformat.BlobProperties;

import java.io.Closeable;

/**
 * @author zifeng
 *
 */
public interface NotificationSystem extends Closeable {
    void onBlobCreated(final String blobId, final BlobProperties blobProperties, final NotificationBlobType
            notificationBlobType);

    void onBlobDeleted(final String blobId, final String serviceId);

    void onBlobReplicaCreated(final String sourceHost, final int port, final String blobId,
            final BlobReplicaSourceType blobReplicaSourceType);

    void onBlobReplicaDeleted(final String sourceHost, final int port, final String blobId,
            final BlobReplicaSourceType blobReplicaSourceType);
}
