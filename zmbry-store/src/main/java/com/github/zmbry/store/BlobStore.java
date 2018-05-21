package com.github.zmbry.store;

import com.github.zmbry.clustermap.ReplicaId;
import com.github.zmbry.clustermap.WriteStatusDelegate;
import com.github.zmbry.config.StoreConfig;
import com.github.zmbry.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.rmi.runtime.Log;

import java.nio.channels.FileLock;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

/**
 * @author zifeng
 *
 */
public class BlobStore implements Store {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    static final String SEPARATOR = "_";
    private final static String lockFile = ".lock";
    private final String mStoreId;
    private final String mDataDir;
    private final ScheduledExecutorService taskScheduler;
    private final ScheduledExecutorService longLivedTaskScheduler;
    private final DiskIOScheduler mDiskIOScheduler;
    private final DiskSpaceAllocator mDiskSpaceAllocator;
    private final Object mStoreWriteLock = new Object();
    private final StoreConfig mStoreConfig;
    private final long mCapacityInBytes;
    private final StoreKeyFactory mStoreKeyFactory;
    private final MessageStoreRecovery mMessageStoreRecovery;
    private final MessageStoreHardDelete mMessageStoreHardDelete;
    private final Time mTime;
    private final UUID sessionId = UUID.randomUUID();
    private final ReplicaId mReplicaId;
    private final WriteStatusDelegate mWriteStatusDelegate;
    private final long thresholdBytesHigh;
    private final long thresholdBytesLow;

    private Log mLog;
    private BlobStoreCompactor mBlobStoreCompactor;
    private PersistentIndex mPersistentIndex;
    private BlobStoreStats mBlobStoreStats;
    private boolean started;
    private FileLock mFileLock;

    private enum MessageWriteSetStateInStore {
        ALL_ABSENT,
        COLLIDING,
        ALL_DUPLICATE,
        SOME_NOT_ALL_DUPLICATE
    }

    BlobStore(ReplicaId replicaId, StoreConfig config, ScheduledExecutorService taskScheduler,
            ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
            DiskSpaceAllocator diskSpaceAllocator, StoreKeyFactory factory, MessageStoreRecovery recovery,
            MessageStoreHardDelete hardDelete, WriteStatusDelegate writeStatusDelegate, Time time) {
        this(replicaId, replicaId.getPartitionId().toString(), config, taskScheduler, longLivedTaskScheduler,
                diskIOScheduler, diskSpaceAllocator, replicaId.getReplicaPath(),
                replicaId.getCapacityInBytes(), factory, recovery, hardDelete, writeStatusDelegate, time);
    }

    private BlobStore(ReplicaId replicaId, String storeId, StoreConfig config, ScheduledExecutorService taskScheduler,
            ScheduledExecutorService longLivedTaskScheduler, DiskIOScheduler diskIOScheduler,
            DiskSpaceAllocator diskSpaceAllocator, String dataDir, long capacityInBytes, StoreKeyFactory factory,
            MessageStoreRecovery recovery, MessageStoreHardDelete hardDelete, WriteStatusDelegate writeStatusDelegate,
            Time time) {
        this.mReplicaId = replicaId;
        this.mStoreId = storeId;
        this.mDataDir = dataDir;
        this.taskScheduler = taskScheduler;
        this.longLivedTaskScheduler = longLivedTaskScheduler;
        this.mDiskIOScheduler = diskIOScheduler;
        this.mDiskSpaceAllocator = diskSpaceAllocator;
        this.mStoreConfig = config;
        this.mCapacityInBytes = capacityInBytes;
        this.mStoreKeyFactory = factory;
        this.mMessageStoreRecovery = recovery;
        this.mMessageStoreHardDelete = hardDelete;
        this.mWriteStatusDelegate = config.storeWriteStatusDelegateEnable ? writeStatusDelegate : null;
        this.mTime = time;
        long threshold = config.storeReadOnlyEnableSizeThresholdPercentage;
        long delta = config.storeReadWriteEnableSizeThresholdPercentageDelta;
        this.thresholdBytesHigh = (long) (capacityInBytes * (threshold / 100.0));
        this.thresholdBytesLow = (long) (capacityInBytes * ((threshold - delta) / 100.0));
    }

    @Override
    public void start() {

    }

    @Override
    public StoreInfo get(final List<? extends StoreKey> ids, final EnumSet<StoreGetOptions> storeGetOptions) {
        return null;
    }

    @Override
    public void put(final MessageWriteSet messageWriteSet) {

    }

    @Override
    public void delete(final MessageWriteSet messageWriteSet) {

    }

    @Override
    public FindInfo findEntriesSince(final FindToken token, final long maxTotalSizeOfEntries) {
        return null;
    }

    @Override
    public Set<StoreKey> findMissingKeys(final List<StoreKey> keys) {
        return null;
    }

    @Override
    public StoreStats getStoreStats() {
        return null;
    }

    @Override
    public boolean isKeyDeleted(final StoreKey key) {
        return false;
    }

    @Override
    public long getSizeInBytes() {
        return 0;
    }

    @Override
    public void shutDown() {

    }

    public boolean isStarted() {
        return started;
    }

    public DiskSpaceRequirements getDiskSpaceRequirements() {
    }
}
