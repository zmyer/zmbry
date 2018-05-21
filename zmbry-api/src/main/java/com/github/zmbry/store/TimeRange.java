package com.github.zmbry.store;

/**
 * @author zifeng
 *
 */
public class TimeRange {

    private final long startTimeInMs;
    private final long endTimeInMs;

    public TimeRange(long referenceTimeInMs, long errorMarginInMs) {
        if (errorMarginInMs < 0 || referenceTimeInMs < 0 || referenceTimeInMs - errorMarginInMs < 0
                || referenceTimeInMs > Long.MAX_VALUE - errorMarginInMs) {
            throw new IllegalArgumentException(
                    "Illegal reference time: " + referenceTimeInMs + " and/or error margin: " + errorMarginInMs);
        }
        startTimeInMs = referenceTimeInMs - errorMarginInMs;
        endTimeInMs = referenceTimeInMs + errorMarginInMs;
    }

    public long getStartTimeInMs() {
        return startTimeInMs;
    }

    public long getEndTimeInMs() {
        return endTimeInMs;
    }
}
