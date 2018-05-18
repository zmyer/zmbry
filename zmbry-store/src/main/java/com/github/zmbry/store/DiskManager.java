package com.github.zmbry.store;

import com.github.zmbry.clustermap.DiskId;
import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.config.DiskManagerConfig;
import com.github.zmbry.config.StoreConfig;

import java.sql.Time;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author zifeng
 *
 */
public class DiskManager {
    private String mDisk;

    public DiskManager(final DiskId diskId, final List<ReplicaId> replicasForDisk, final StoreConfig storeConfig,
            final DiskManagerConfig diskManagerConfig, final ScheduledExecutorService scheduledExecutorService,
            final StoreKeyFactory storeKeyFactory, final MessageStoreRecovery recovery,
            final MessageStoreHardDelete hardDelete, final WriteStatusDelegate writeStatusDelegate, final Time time) {
    }

    public void start() {
    }

    public String getDisk() {
        return mDisk;
    }

    public void shutDown() {
    }
}
