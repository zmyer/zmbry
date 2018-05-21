package com.github.zmbry.store;

import com.github.zmbry.clustermap.DiskId;
import com.github.zmbry.clustermap.PartitionId;
import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.clustermap.WriteStatusDelegate;
import com.github.zmbry.config.DiskManagerConfig;
import com.github.zmbry.config.StoreConfig;
import com.github.zmbry.utils.Throttler;
import com.github.zmbry.utils.Time;
import com.github.zmbry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zifeng
 *
 */
public class DiskManager {
    private static final Logger logger = LoggerFactory.getLogger(DiskManager.class);
    private final Map<PartitionId, BlobStore> mStores = new HashMap<>();
    private final DiskId mDisk;
    private final Time mTime;
    private final DiskIOScheduler mDiskIOScheduler;
    private final ScheduledExecutorService mScheduledExecutorService;
    private final DiskSpaceAllocator mDiskSpaceAllocator;
    private final CompactionManager mCompactionManager;
    private boolean running = false;
    static final String CLEANUP_OPS_JOB_NAME = "cleanupOps";


    public DiskManager(final DiskId diskId, final List<ReplicaId> replicasForDisk, final StoreConfig storeConfig,
            final DiskManagerConfig diskManagerConfig, final ScheduledExecutorService scheduledExecutorService,
            final StoreKeyFactory storeKeyFactory, final MessageStoreRecovery recovery,
            final MessageStoreHardDelete hardDelete, final WriteStatusDelegate writeStatusDelegate, final Time time) {
        this.mDisk = diskId;
        this.mTime = time;
        this.mDiskIOScheduler = new DiskIOScheduler(getThrottlers(storeConfig, time));
        mScheduledExecutorService = Utils.newScheduler(1, true);
        mDiskSpaceAllocator = new DiskSpaceAllocator(diskManagerConfig.diskManagerEnableSegmentPooling, new File
                (diskId.getMountPath(), diskManagerConfig.diskManagerReserveFileDirName), diskManagerConfig
                .diskManagerRequiredSwapSegmentsPerSize);
        for (final ReplicaId replicaId : replicasForDisk) {
            if (diskId.equals(replicaId.getDiskId())) {
                BlobStore blobStore = new BlobStore(replicaId, storeConfig, scheduledExecutorService,
                        mScheduledExecutorService, mDiskIOScheduler, mDiskSpaceAllocator, storeKeyFactory, recovery,
                        hardDelete, writeStatusDelegate, time);
                mStores.put(replicaId.getPartitionId(), blobStore);
            }
        }
        mCompactionManager = new CompactionManager(diskId.getMountPath(), storeConfig, mStores.values(), time);
    }

    private Map<String, Throttler> getThrottlers(final StoreConfig storeConfig, final Time time) {
        Map<String, Throttler> throttlers = new HashMap<>();
        Throttler cleanOpsThrottler = new Throttler(storeConfig.storeCleanupOperationsBytesPerSec, -1, true, time);
        throttlers.put(CLEANUP_OPS_JOB_NAME, cleanOpsThrottler);
        Throttler statsIndexScanThrottler = new Throttler(storeConfig.storeStatsIndexEntriesPerSecond, 1000, true,
                time);
        throttlers.put(BlobStoreStats.IO_SCHEDULER_JOB_TYPE, statsIndexScanThrottler);
        return throttlers;
    }

    public void start() throws InterruptedException {
        long startTimeMs = mTime.milliseconds();
        final AtomicInteger numStoreFailures = new AtomicInteger(0);
        checkMountPathAccessible();
        List<Thread> startUpThreads = new ArrayList<>();
        for (final Map.Entry<PartitionId, BlobStore> partitionIdBlobStoreEntry : mStores.entrySet()) {
            Thread thread = Utils.newThread("store-startup-" + partitionIdBlobStoreEntry.getKey(), () -> {
                try {
                    partitionIdBlobStoreEntry.getValue().start();
                } catch (Exception e) {
                    numStoreFailures.incrementAndGet();
                    logger.error(
                            "Exception while starting store for the partition" + partitionIdBlobStoreEntry.getKey(),
                            e);
                }
            }, false);
            thread.start();
            startUpThreads.add(thread);
        }
        for (final Thread thread : startUpThreads) {
            thread.join();
        }
        if (numStoreFailures.get() > 0) {
            logger.error(
                    "Could not start " + numStoreFailures.get() + " out of " + mStores.size() +
                            " stores on the disk " + getDisk());
        }
        List<DiskSpaceRequirements> requirementsList = new ArrayList<>();
        for (final BlobStore blobStore : mStores.values()) {
            if (blobStore.isStarted()) {
                DiskSpaceRequirements requirements = blobStore.getDiskSpaceRequirements();
                if (requirements != null) {
                    requirementsList.add(requirements);
                }
            }
        }
        mDiskSpaceAllocator.initializePool(requirementsList);
        mCompactionManager.enable();
        running = true;
    }

    private void checkMountPathAccessible() {
    }

    public DiskId getDisk() {
        return mDisk;
    }

    public void shutDown() throws InterruptedException {
        long startTimeMs = mTime.milliseconds();
        try {
            running = false;
            mCompactionManager.disable();
            mDiskIOScheduler.disable();
            final AtomicInteger failures = new AtomicInteger(0);
            List<Thread> shutDownThreads = new ArrayList<>();
            for (final Map.Entry<PartitionId, BlobStore> partitionIdBlobStoreEntry : mStores.entrySet()) {
                Thread thread = Utils.newThread("store-shutdown-" + partitionIdBlobStoreEntry.getKey(), () -> {
                    try {
                        partitionIdBlobStoreEntry.getValue().shutDown();
                    } catch (Exception e) {
                        failures.incrementAndGet();
                        logger.error("Exception while shutting down store {} on disk {}",
                                partitionIdBlobStoreEntry.getKey(),
                                mDisk, e);
                    }
                }, false);
                thread.start();
                shutDownThreads.add(thread);
            }
            for (final Thread thread : shutDownThreads) {
                thread.join();
            }
            if (failures.get() > 0) {
                logger.error(
                        "Could not shutdown " + failures.get() + " out of " + mStores.size() +
                                " stores on the disk " + mDisk);
            }
            mCompactionManager.awaitTermination();
            mScheduledExecutorService.shutdown();
            if (!mScheduledExecutorService.awaitTermination(30, TimeUnit.SECONDS)) {
                logger.error("Could not terminate long live tasks after DiskManager shutdown");
            }
        } finally {

        }
    }

    public Store getStore(final PartitionId partition) {
        BlobStore blobStore = mStores.get(partition);
        return (running && blobStore != null && blobStore.isStarted()) ? blobStore : null;
    }
}
