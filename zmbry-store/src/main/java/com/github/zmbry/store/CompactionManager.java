package com.github.zmbry.store;

import com.github.zmbry.config.StoreConfig;
import com.github.zmbry.utils.Time;
import com.github.zmbry.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author zifeng
 *
 */
public class CompactionManager {
    private static final String THREAD_NAME_PREFIX = "StoreCompactionThread-";

    public void enable() {
    }

    public void awaitTermination() {
    }

    public void disable() {
    }

    private enum Trigger {
        PERIODIC,
        ADMIN
    }

    private final String mMountPath;
    private final StoreConfig mStoreConfig;
    private final Time mTime;
    private final Collection<BlobStore> mStores;
    private final CompactionExecutor mCompactionExecutor;
    private final CompactionPolicy mCompactionPolicy;
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private Thread compactionThread;


    public CompactionManager(final String mountPath, final StoreConfig storeConfig, final Collection<BlobStore> stores,
            final Time time) {
        this.mMountPath = mountPath;
        this.mStoreConfig = storeConfig;
        this.mStores = stores;
        this.mTime = time;
        if (!mStoreConfig.storeCompactionTriggers[0].isEmpty()) {
            EnumSet<Trigger> triggers = EnumSet.noneOf(Trigger.class);
            for (final String trigger : mStoreConfig.storeCompactionTriggers) {
                triggers.add(Trigger.valueOf(trigger.toUpperCase()));
            }
            mCompactionExecutor = new CompactionExecutor(triggers);
            try {
                CompactionPolicyFactory compactionPolicyFactory = Utils.getObj(mStoreConfig
                        .storeCompactionPolicyFactory, mStoreConfig, mTime);
                mCompactionPolicy = compactionPolicyFactory.getCompactionPolicy();
            } catch (Exception e) {
                throw new IllegalStateException("Error creating compaction policy using compactionPolicyFactory "
                        + storeConfig.storeCompactionPolicyFactory);
            }
        } else {
            mCompactionExecutor = null;
            mCompactionPolicy = null;
        }
    }

    private class CompactionExecutor implements Runnable {
        private final Logger logger = LoggerFactory.getLogger(getClass());
        private final EnumSet<Trigger> mTriggers;
        private final ReentrantLock lock = new ReentrantLock();
        private final Condition waitCondition = lock.newCondition();
        private final Set<BlobStore> storeToSkip = new HashSet<>();
        private final Set<BlobStore> storesDisableCompaction = ConcurrentHashMap.newKeySet();
        private final LinkedBlockingDeque<BlobStore> storesToCheck = new LinkedBlockingDeque<>();
        private final long waitTimes = TimeUnit.HOURS.toMillis(mStoreConfig.storeCompactionCheckFrequencyInHours);
        private volatile boolean enable = true;
        volatile boolean isRunning = false;

        CompactionExecutor(final EnumSet<Trigger> triggers) {
            mTriggers = triggers;
        }

        @Override
        public void run() {
            isRunning = true;
            try {
                for (BlobStore blobStore : mStores) {
                    if (blobStore.isStarted()) {
                        try {
                            blobStore.maybeResumeCompaction();
                        } catch (Exception e) {
                            storeToSkip.add(blobStore);
                        }
                    }
                }
                long exceptedNextCheckTime = mTime.milliseconds() + waitTimes;
                if (mTriggers.contains(Trigger.PERIODIC)) {
                    storesToCheck.addAll(mStores);
                }
                while (enable) {
                    try {
                        while (enable && storesToCheck.peek() != null) {
                            BlobStore blobStore = storesToCheck.poll();
                            boolean compactionStarted = false;
                            try {
                                if (blobStore.isStarted() && !storeToSkip.contains(blobStore)
                                        && !storesDisableCompaction.contains(blobStore)) {
                                    CompactionDetails details = getCompactionDetails(blobStore);
                                    if (details != null) {
                                        compactionStarted = true;
                                        blobStore.compact(details);
                                    } else {
                                        logger.info("{} is not eligible for compaction", blobStore);
                                    }
                                }
                            } catch (Exception e) {
                                logger.error("Compaction of store {} failed. Continuing with the next store", blobStore,
                                        e);
                                storeToSkip.add(blobStore);
                            }
                        }
                        lock.lock();
                        try {
                            if (enable) {
                                if (storesToCheck.peek() == null) {
                                    if (mTriggers.contains(Trigger.PERIODIC)) {
                                        long actualWaitTime = exceptedNextCheckTime - mTime.milliseconds();
                                        mTime.await(waitCondition, actualWaitTime);
                                    } else {
                                        waitCondition.await();
                                    }
                                }
                                if (mTriggers.contains(Trigger.PERIODIC) && mTime.milliseconds() >
                                        exceptedNextCheckTime) {
                                    exceptedNextCheckTime = mTime.milliseconds() + waitTimes;
                                    storesToCheck.addAll(mStores);
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    } catch (Exception e) {
                        logger.error("Compaction executor for {} encountered an error. Continuing", mMountPath, e);
                    }
                }
            } finally {
                isRunning = false;
                logger.info("Stopping compaction thread for {}", mMountPath);
            }
        }
    }
}
