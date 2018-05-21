package com.github.zmbry.store;

import com.github.zmbry.utils.Throttler;

import java.util.Map;

/**
 * @author zifeng
 *
 */
public class DiskIOScheduler {
    private final Map<String, Throttler> mThrottlers;

    DiskIOScheduler(Map<String, Throttler> throttlers) {
        this.mThrottlers = throttlers;
    }

    long getSlice(String jobType, String jobId, long usedSinceLastCall) {
        Throttler throttler = mThrottlers.get(jobType);
        if (throttler != null) {
            try {
                throttler.maybeThrottle(usedSinceLastCall);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Throttler call interrupted", e);
            }
        }
        return Long.MAX_VALUE;
    }

    void disable() {
        for (Throttler throttler : mThrottlers.values()) {
            throttler.disable();
        }
    }
}
