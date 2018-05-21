package com.github.zmbry.store;

import com.github.zmbry.clustermap.DiskId;
import com.github.zmbry.clustermap.PartitionId;
import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.clustermap.WriteStatusDelegate;
import com.github.zmbry.config.DiskManagerConfig;
import com.github.zmbry.config.StoreConfig;
import com.github.zmbry.utils.Time;
import com.github.zmbry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zifeng
 *
 */
public class StorageManager {
    private static final Logger logger = LoggerFactory.getLogger(StorageManager.class);
    private final Map<PartitionId, DiskManager> mPartitionIdDiskManagerMap = new HashMap<>();
    private final List<DiskManager> mDiskManagers = new ArrayList<DiskManager>();
    private final Time mTime;

    public StorageManager(StoreConfig storeConfig, DiskManagerConfig diskManagerConfig, ScheduledExecutorService
            scheduledExecutorService, List<? extends ReplicaId> replicaIds, StoreKeyFactory storeKeyFactory,
            MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, WriteStatusDelegate
            writeStatusDelegate, Time time) throws StoreException {
        verifyConfigs(storeConfig, diskManagerConfig);
        this.mTime = time;
        Map<DiskId, List<ReplicaId>> diskToReplicaMap = new HashMap<DiskId, List<ReplicaId>>();
        for (final ReplicaId replicaId : replicaIds) {
            DiskId disk = replicaId.getDiskId();
            diskToReplicaMap.computeIfAbsent(disk, key -> new ArrayList<ReplicaId>()).add(replicaId);
        }
        for (Map.Entry<DiskId, List<ReplicaId>> entry : diskToReplicaMap.entrySet()) {
            DiskId diskId = entry.getKey();
            List<ReplicaId> replicasForDisk = entry.getValue();
            DiskManager diskManager = new DiskManager(diskId, replicasForDisk, storeConfig, diskManagerConfig,
                    scheduledExecutorService, storeKeyFactory, recovery, hardDelete, writeStatusDelegate, time);
            mDiskManagers.add(diskManager);
            for (final ReplicaId replicaId : replicasForDisk) {
                mPartitionIdDiskManagerMap.put(replicaId.getPartitionId(), diskManager);
            }
        }
    }

    private void verifyConfigs(final StoreConfig storeConfig, final DiskManagerConfig diskManagerConfig)
            throws StoreException {
        if (storeConfig.storeDeletedMessageRetentionDays
                < TimeUnit.SECONDS.toDays(storeConfig.storeDataFlushIntervalSeconds) + 1) {
            throw new StoreException("Message retention days must be greater than the store flush interval period",
                    StoreErrorCodes.Initialization_Error);
        }
        if (diskManagerConfig.diskManagerReserveFileDirName.length() == 0) {
            throw new StoreException("Reserve file directory name is empty", StoreErrorCodes.Initialization_Error);
        }
    }

    public void start() throws InterruptedException {
        long startTimeMs = mTime.milliseconds();
        try {
            logger.info("Starting storage manager");
            List<Thread> startUpThreads = new ArrayList<>();
            for (final DiskManager diskManager : mDiskManagers) {
                Thread thread = Utils.newThread("disk-manager-startUp-" + diskManager.getDisk(), () -> {
                    try {
                        diskManager.start();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }, false);
                thread.start();
                startUpThreads.add(thread);
            }
            for (final Thread startUpThread : startUpThreads) {
                startUpThread.join();
            }
            logger.info("Starting storage manager complete");
        } finally {

        }
    }

    public void shutDown() throws InterruptedException {
        long startTimeMs = mTime.milliseconds();
        try {
            logger.info("Shutting down storage manager");
            List<Thread> shutDownThreads = new ArrayList<>();
            for (final DiskManager diskManager : mDiskManagers) {
                Thread thread = Utils.newThread("disk-manager-shutdown-" + diskManager.getDisk(), () -> {
                    try {
                        diskManager.shutDown();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }, false);
                thread.start();
                shutDownThreads.add(thread);
            }
            for (final Thread thread : shutDownThreads) {
                thread.join();
            }
            logger.info("Shutting down storage manager complete");
        } finally {

        }
    }

    public Store getStore(final PartitionId partition) {
        DiskManager diskManager = mPartitionIdDiskManagerMap.get(partition);
        return diskManager != null ? diskManager.getStore(partition) : null;
    }
}
