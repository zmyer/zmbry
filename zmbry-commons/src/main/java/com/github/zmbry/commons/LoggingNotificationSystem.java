package com.github.zmbry.commons;

import com.github.zmbry.messageformat.BlobProperties;
import com.github.zmbry.notification.BlobReplicaSourceType;
import com.github.zmbry.notification.NotificationBlobType;
import com.github.zmbry.notification.NotificationSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author zifeng
 *
 */
public class LoggingNotificationSystem implements NotificationSystem {
    private final Logger logger = LoggerFactory.getLogger(getClass());

    @Override
    public void onBlobCreated(final String blobId, final BlobProperties blobProperties,
            final NotificationBlobType notificationBlobType) {
        logger.debug("onBlobCreated " + blobId + ", " + blobProperties + ", " + notificationBlobType);
    }

    @Override
    public void onBlobDeleted(final String blobId, final String serviceId) {
        logger.debug("onBlobDeleted " + blobId, ", " + serviceId);
    }

    @Override
    public void onBlobReplicaCreated(final String sourceHost, final int port, final String blobId,
            final BlobReplicaSourceType blobReplicaSourceType) {
        logger.debug("onBlobReplicaCreated " + sourceHost + ", " + port + ", " + blobId + ", " + blobReplicaSourceType);
    }

    @Override
    public void onBlobReplicaDeleted(final String sourceHost, final int port, final String blobId,
            final BlobReplicaSourceType blobReplicaSourceType) {
        logger.debug("onBlobReplicaCreated " + sourceHost + ", " + port + ", " + blobId + ", " + blobReplicaSourceType);
    }

    @Override
    public void close() throws IOException {

    }
}
